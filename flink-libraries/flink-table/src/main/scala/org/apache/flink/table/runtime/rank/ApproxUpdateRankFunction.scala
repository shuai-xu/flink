/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.runtime.rank

import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.api.{StreamQueryConfig, TableException}
import org.apache.flink.table.codegen.GeneratedSorter
import org.apache.flink.table.plan.util.RankLimit
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.ProcessFunction.Context
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

class ApproxUpdateRankFunction(
    inputRowType: BaseRowTypeInfo[_],
    rowKeyType: BaseRowTypeInfo[_],
    rowKeySelector: KeySelector[BaseRow, BaseRow],
    gSorter: GeneratedSorter,
    sortKeySelector: KeySelector[BaseRow, BaseRow],
    outputArity: Int,
    rankKind: SqlKind,
    rankLimit: RankLimit,
    cacheSize: Long,
    approxBufferMultiplier: Long,
    approxBufferMinSize: Long,
    generateRetraction: Boolean,
    queryConfig: StreamQueryConfig)
  extends AbstractUpdateRankFunction(
    inputRowType,
    rowKeyType,
    gSorter,
    sortKeySelector,
    outputArity,
    rankLimit,
    cacheSize,
    generateRetraction,
    queryConfig) {

  // indicate whether sort map have overflowed when adding new record exceeds its limit
  private var isSortMapOverFlowed: Boolean = false

  override def processElement(
    inputBaseRow: BaseRow,
    context: Context,
    out: Collector[BaseRow]): Unit = {

    val currentTime = context.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(context, currentTime)

    initHeapStates()
    initRankEnd(inputBaseRow)

    if (isRowNumberAppend || hasOffset) {
      // the without-number-algorithm can't handle topn with offset,
      // so use the with-number-algorithm to handle offset
      processElementWithRowNumber(inputBaseRow, out)
    } else {
      processElementWithoutRowNumber(inputBaseRow, out)
    }
  }

  private def processElementWithRowNumber(inputRow: BaseRow, out: Collector[BaseRow]): Unit = {
    val sortKey = sortKeySelector.getKey(inputRow)
    val rowKey = rowKeySelector.getKey(inputRow)

    if (rowKeyMap.containsKey(rowKey)) {
      // it is an updated record which is in the topN
      val oldRow = rowKeyMap.get(rowKey)
      val oldSortKey = sortKeySelector.getKey(oldRow.row)
      if (oldSortKey.equals(sortKey)) {
        // this updated row only changed its content,
        // with sort key(and rank as a consequence) being the same
        rankKind match {
          case SqlKind.ROW_NUMBER =>
            // sort key is not changed, so the rank is the same, only output the row
            val (rank, innerRank) = rowNumber(sortKey, rowKey, sortedMap)
            rowKeyMap.put(rowKey, RankRow(inputRow.copy(), innerRank, dirty = true))
            retract(out, oldRow.row, rank) // retract old record
            collect(out, inputRow, rank)   // and then emit updated record

          case _ => ???
        }
        return   // All done in this scenario
      }

      // update in-memory map
      sortedMap.remove(oldSortKey, rowKey)
      val innerSize = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRow.copy(), innerSize, dirty = true))
      updateInnerRank(oldSortKey)   // update inner rank of records under the old sort key

      // emit records
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          updateRecordsWithRowNumber(sortKey, inputRow, out, oldSortKey, oldRow.innerRank)

        case _ => ???
      }
    } else if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
      // it is an unique record but is in the topN, insert sort key into sortedMap
      val innerSize = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRow.copy(), innerSize, dirty = true))

      // emit records
      rankKind match {
        case SqlKind.ROW_NUMBER =>
          emitRecordsWithRowNumber(sortKey, inputRow, out)

        case _ => ???
      }
    } else {
      // buffer is full
    }
  }

  private def processElementWithoutRowNumber(
    inputRow: BaseRow,
    out: Collector[BaseRow]): Unit = {

    val sortKey = sortKeySelector.getKey(inputRow)
    val rowKey = rowKeySelector.getKey(inputRow)
    if (rowKeyMap.containsKey(rowKey)) {
      // it is an updated record which is in the topN
      val oldRow = rowKeyMap.get(rowKey)
      val oldSortKey = sortKeySelector.getKey(oldRow.row)
      val (oldRank, _) = rowNumber(oldSortKey, rowKey, sortedMap)
      if (!oldSortKey.equals(sortKey)) {
        // remove old sort key
        sortedMap.remove(oldSortKey, rowKey)
        // add new sort key
        val size = sortedMap.put(sortKey, rowKey)
        rowKeyMap.put(rowKey, RankRow(inputRow.copy, size, dirty = true))
        // update inner rank of records under the old sort key
        updateInnerRank(oldSortKey)
      }

      val (rank, _) = rowNumber(sortKey, rowKey, sortedMap)
      if (!isInRankEnd(rank) && isInRankEnd(oldRank)) {
        // updated row moves from topn range into out of topn range within buffer limit
        val lastRowKeyInTopn = sortedMap.getElement(rankEnd.toInt)
        val lastRowInTopn = rowKeyMap.get(lastRowKeyInTopn)
        if (lastRowKeyInTopn == null || lastRowInTopn == null) {
          throw new TableException("This shouldn't happen. Please file an issue.")
        }
        //emit row in last of topn that is squeezed in from out of topn range
        collect(out, lastRowInTopn.row)
        //note that we shall also send delete msg of old row before update
        delete(out, oldRow.row)
      } else if (isInRankEnd(rank) && !isInRankEnd(oldRank)) {
        // updated row moves from out of topn range into topn range within buffer limit
        val outrangeRowKey = sortedMap.getElement(rankEnd.toInt + 1)
        val outrangeRow = rowKeyMap.get(outrangeRowKey)
        if (outrangeRowKey == null || outrangeRow == null) {
          throw new TableException("This shouldn't happen. Please file an issue.")
        }
        // emit updated row
        collect(out, inputRow)
        // send delete msg of row that is squeezed out of topn range
        delete(out, outrangeRow.row)
      } else if (isInRankEnd(rank)) {
        // updated row move within topn range
        collect(out, inputRow)   //emit this updated row
      }
    } else if (checkSortKeyInBufferRange(sortKey, sortedMap, sortKeyComparator)) {
      // it is an unique record but is in the topN
      // insert sort key into sortedMap
      val size = sortedMap.put(sortKey, rowKey)
      rowKeyMap.put(rowKey, RankRow(inputRow.copy, size, dirty = true))
      val (rank, _) = rowNumber(sortKey, rowKey, sortedMap)
      if (isInRankEnd(rank)) {
        collect(out, inputRow)   //emit this new row

        val tempRowkey = sortedMap.getElement(rankEnd.toInt + 1)
        if (tempRowkey != null) {
          // if there's row moved out of topn as consequence of inserting new row in topn range,
          // send delete msg to downstream
          val tempRow = rowKeyMap.get(tempRowkey)
          delete(out, tempRow.row)
        }
      }
      // remove retired element
      if (sortedMap.currentTopNum > getMaxSortMapSize) {
        val lastRowKey = sortedMap.removeLast()
        if (lastRowKey != null) {
          val lastRow = rowKeyMap.remove(lastRowKey)
          dataState.remove(executionContext.currentKey(), lastRowKey)
        }

        if (!isSortMapOverFlowed) {
          // mark sort map as overflowed
          isSortMapOverFlowed = true
        }
      }
    } else {
      // buffer is full
    }
  }

  def updateRecordsWithRowNumber(
    sortKey: BaseRow,
    inputRow: BaseRow,
    out: Collector[BaseRow],
    oldSortKey: BaseRow = null,
    oldInnerRank: Int = -1): Unit = {

    // determine if scenario is forward-update or backward-update
    val updateForward: Boolean = if (sortKeyComparator.compare(sortKey, oldSortKey) < 0) {
      true
    } else {
      false
    }
    val minKey = if (updateForward) {
      sortKey
    } else {
      oldSortKey
    }
    val maxKey = if (updateForward) {
      oldSortKey
    } else {
      sortKey
    }

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0

    while (iterator.hasNext && isInRankEnd(curRank + 1)) {
      val entry = iterator.next()
      val curKey = entry.getKey
      val rowKeys = entry.getValue

      val compareWithMin = sortKeyComparator.compare(curKey, minKey)
      val compareWithMax = sortKeyComparator.compare(curKey, maxKey)

      if (compareWithMin < 0) {
        // elements in the front that don't change rank
        curRank += rowKeys.size()
      } else if (compareWithMax > 0) {
        // now we reach after max key. All is finished. return directly
        return
      } else {

        if (updateForward && compareWithMin == 0) {
          // In forward-update scenario, we are not in sort key, emit updated row
          curRank += rowKeys.size()
          collect(out, inputRow, curRank)
        } else {
          /* Below while loop handles 2 situations:
           * 1. updated row emit in backward-update scenario
           * 2. retract & emit rows that have rank change due to updated row,
           *    both in forward and afterward update scenario
           * NOTE:
           *  when reaching end of rank-changed section, it'll return directly
           */
          val rowKeyIter = rowKeys.iterator()
          var curInnerRank = 0
          while (rowKeyIter.hasNext && isInRankEnd(curRank + 1)) {
            curRank += 1
            curInnerRank += 1

            if (updateForward) {
              if (compareWithMax == 0 && curInnerRank >= oldInnerRank) {
                // In forward-update scenario, match to the previous position , all done and return
                return
              }

              val rowKey = rowKeyIter.next()
              val tempRow = rowKeyMap.get(rowKey)
              retract(out, tempRow.row, curRank - 1)
              collect(out, tempRow.row, curRank)
            } else {
              if (compareWithMax == 0 && curInnerRank == rowKeys.size()) {
                // In backward-update scenario, match to new position,
                // emit updated row, all done and return
                collect(out, inputRow, curRank)
                return
              } else if (!(compareWithMin == 0 && curInnerRank < oldInnerRank)) {
                val rowKey = rowKeyIter.next()
                val tempRow = rowKeyMap.get(rowKey)
                if (curRank < rankEnd) {
                  //It should take into consideration of (topn + 1) changed to (topn),
                  // in which retract is not needed as we didn't emit (topn + 1) previously
                  retract(out, tempRow.row, curRank + 1)
                }
                collect(out, tempRow.row, curRank)
              } else {
                // just move forward. (handles elem in list of node "min key" specified above
                rowKeyIter.next()
              }
            }
          }
        }

      }
    }
  }

  def emitRecordsWithRowNumber(
    sortKey: BaseRow,
    inputRow: BaseRow,
    out: Collector[BaseRow]): Unit = {

    val iterator = sortedMap.entrySet().iterator()
    var curRank = 0
    var findSortKey = false

    while (iterator.hasNext && isInRankEnd(curRank + 1)) {
      val entry = iterator.next()
      val curKey = entry.getKey
      val rowKeys = entry.getValue

      if (!findSortKey && curKey.equals(sortKey)) {
        curRank += rowKeys.size()
        collect(out, inputRow, curRank) //emit the updated row in the right rank pos
        findSortKey = true
      } else if (findSortKey) {
        // emit updates for all rows in the topn that rank after this new row
        val rowKeyIter = rowKeys.iterator()
        while (rowKeyIter.hasNext && isInRankEnd(curRank + 1)) {
          curRank += 1
          val rowKey = rowKeyIter.next()
          val tempRow = rowKeyMap.get(rowKey)
          retract(out, tempRow.row, curRank - 1)
          collect(out, tempRow.row, curRank)
        }
      } else {
        curRank += rowKeys.size()
      }
    }

    // remove retired element
    if (sortedMap.currentTopNum > getMaxSortMapSize) {
      val lastRowKey = sortedMap.removeLast()
      if (lastRowKey != null) {
        rowKeyMap.remove(lastRowKey)
        dataState.remove(executionContext.currentKey(), lastRowKey)
      }

      if (!isSortMapOverFlowed) {
        // mark sort map as overflowed
        isSortMapOverFlowed = true
      }
    }
  }

  override protected def getMaxSortMapSize: Long = {
    if (isConstantRankEnd) {
      if (approxBufferMinSize == 0) {
        // meaning no lower threshold
        rankEnd * approxBufferMultiplier
      } else {
        // make sure lower limit is guaranteed
        math.max(rankEnd * approxBufferMultiplier, approxBufferMinSize)
      }
    } else {
      // no constant rank, keep size config in line with update rank
      100 * approxBufferMultiplier
    }
  }

}
