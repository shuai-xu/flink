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

package org.apache.flink.table.plan.nodes.physical.stream

import java.util
import java.util.concurrent.TimeUnit

import com.alibaba.blink.table.sources.HBaseDimensionTableSource
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.PartitionUtils
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class StreamExecMultiJoinHTables(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    var rowRelDataType: RelDataType,
    val joinFilter: RexNode,
    var leftNode: RelNode,
    var requiredLeftFieldIdx: Array[Int],
    val hTableSources: util.LinkedList[HBaseDimensionTableSource[_]],
    val projFields: util.List[ImmutableBitSet],
    var leftKeyList: util.LinkedList[String],
    val joinConditions: util.List[RexNode],
    val joinTypes: util.LinkedList[JoinRelType],
    val postJoinFilter: RexNode,
    val joinFieldRefCountsMap: util.Map[Int, ImmutableIntList],
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, leftNode)
  with StreamExecRel {

  def containsOuter(): Boolean =
    joinTypes.asScala.find(_ != JoinRelType.INNER).map(_ => true).getOrElse(false)

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecMultiJoinHTables(
      cluster,
      traitSet,
      getRowType,
      joinFilter,
      inputs.get(0),
      requiredLeftFieldIdx,
      hTableSources,
      projFields,
      leftKeyList,
      joinConditions,
      joinTypes,
      postJoinFilter,
      joinFieldRefCountsMap,
      ruleDescription
    )
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), multi-join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    .item("where", joinConditionToString)
    .item("join", joinSelectionToString)
    .item("joinType", joinTypeToString)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    assert(hTableSources.size != 0)
    val elementRate = 100.0d/hTableSources.size //merge n requests
    planner.getCostFactory.makeCost(elementRate, elementRate, elementRate)
  }

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return DataStream of type [[Row]]
    */
  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    // we should use physical row type info here, exclude time indicator added by system
    val inputSchema = new BaseRowSchema(getInput.getRowType)
    val inputRowTypeInfo = inputSchema.typeInfo(classOf[BaseRow])
    val originalInputLen = inputRowTypeInfo.getArity

    for (joinType <- joinTypes.asScala) {
      if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
        throw TableException("Only support inner or left join with a HBaseTable.")
      }
    }
    val inputDataStream = new DataStream(
      tableEnv.execEnv,
      getInput.asInstanceOf[StreamExecRel].translateToPlan(tableEnv))
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    class IntermediateSourceChain {
      val chainedSources: util.List[HBaseDimensionTableSource[_]] =
        new util.ArrayList[HBaseDimensionTableSource[_]]()
      val chainedJoinTypes: util.List[JoinRelType] = new util.ArrayList[JoinRelType]()
      val chainedLeftJoinKeys: util.List[String] = new util.ArrayList[String]()
      var chainedLeftJoinKeyIndexes: util.List[Integer] = new util.ArrayList[Integer]()
      var chainedLeftJoinKeyTypes: util.List[TypeInformation[_]] =
        new util.ArrayList[TypeInformation[_]]()
      var strongConsistencyOnChain: Boolean = false
    }

    def translateOneChain(
        inputRowTypeInfo: BaseRowTypeInfo[_],
        originalInputLen: Int,
        requiredLeftFieldIdx: Array[Int],
        inputDataStream: DataStream[BaseRow],
        chain: IntermediateSourceChain)
    : (BaseRowTypeInfo[_], Int, Array[Int], DataStream[BaseRow]) = {

      // calc this chain's output type, merge input and all table sources
      val hTableFields = chain.chainedSources.asScala
        .foldLeft((Array.empty[String], Array.empty[TypeInformation[_]])) {
          (acc, i) => {
            if (i.isSelectKey) {
              (acc._1 ++ i.getFieldNames, acc._2 ++ i.getFieldTypes)
            } else {
              (acc._1 ++ i.getFieldNames.slice(1, i.getFieldNames.length),
                acc._2 ++ i.getFieldTypes.slice(1, i.getFieldTypes.length))
            }
          }
        }

      val leftFieldIdxSet = requiredLeftFieldIdx.toSet
      val projectedLeftFieldNames = inputRowTypeInfo.getFieldNames.zipWithIndex
        .filter(f => leftFieldIdxSet.contains(f._2)).map(_._1)
      val projectedLeftFieldTypes = inputRowTypeInfo.getFieldTypes.zipWithIndex
        .filter(f => leftFieldIdxSet.contains(f._2)).map(_._1)

      val newOutputRowInfo = new BaseRowSchema(typeFactory.buildLogicalRowType(
        projectedLeftFieldNames ++ hTableFields._1,
        projectedLeftFieldTypes ++ hTableFields._2))
      val newInputRowTypeInfo = new BaseRowSchema(newOutputRowInfo.relDataType)
        .typeInfo(classOf[GenericRow])
      val newBaseOutputRowInfo = newOutputRowInfo.typeInfo(classOf[GenericRow])

      val (keys, types) = {
        val keys = new util.LinkedList[Integer]()
        val keyTypes = new util.ArrayList[TypeInformation[_]]()
        val leftInputWithIdx = inputRowTypeInfo.getFieldNames.zipWithIndex
        for (i <- 0 until chain.chainedLeftJoinKeys.size()) {
          val (idx, keyType) = leftInputWithIdx.find(_._1 == (chain.chainedLeftJoinKeys.get(i)))
            .map(_._2) match {
            case Some(idx) => (idx, inputRowTypeInfo.getTypeAt(idx))
            case _ => throw TableException(
              "This must be a bug, could not match left join key " +
                "index from root input when start join a HTable")
          }
          keys.add(idx)
          keyTypes.add(keyType)
        }
        (keys, keyTypes)
      }

      chain.chainedLeftJoinKeyIndexes = keys
      chain.chainedLeftJoinKeyTypes = types

      val joinTotalFields = chain.chainedSources.asScala.foldLeft(0) {
        (acc, h) => acc + h.getOutputFieldsNumber
      }
      val requiredLeftFieldLen = requiredLeftFieldIdx.length
      val newInputLen = requiredLeftFieldLen + joinTotalFields
      // calc new field indexes for left input
      val newRequiredLeftFieldIdx = new Array[Int](newInputLen)
      for (i <- 0 until newInputLen) {
        if (i < requiredLeftFieldLen) {
          newRequiredLeftFieldIdx(i) = requiredLeftFieldIdx(i)
        } else {
          newRequiredLeftFieldIdx(i) = originalInputLen + (i - requiredLeftFieldLen)
        }
      }

      val tableSource = chain.chainedSources.get(0)

      val asyncFunction = if (chain.chainedSources.size() > 1) {
        tableSource.getAsyncMultiFetchFunction[BaseRow, BaseRow](
          newBaseOutputRowInfo.asInstanceOf[TypeInformation[BaseRow]],
          requiredLeftFieldIdx,
          requiredLeftFieldIdx.map(inputRowTypeInfo.getTypeAt(_)),
          chain.chainedSources,
          chain.chainedJoinTypes,
          chain.chainedLeftJoinKeyIndexes,
          chain.chainedLeftJoinKeyTypes,
          chain.strongConsistencyOnChain,
          tableEnv.execEnv.getConfig.isObjectReuseEnabled)
      } else {
        assert(chain.chainedSources.size() == 1)
        val leftKeyIdx = chain.chainedLeftJoinKeyIndexes.get(0)
        val leftKeyType = chain.chainedLeftJoinKeyTypes.get(0)
        tableSource.getAsyncFetchFunction[BaseRow, BaseRow](
          newBaseOutputRowInfo.asInstanceOf[TypeInformation[BaseRow]],
          chain.chainedJoinTypes.get(0),
          leftKeyIdx,
          leftKeyType,
          chain.strongConsistencyOnChain,
          tableEnv.execEnv.getConfig.isObjectReuseEnabled)
      }

      // if exists partitioning enabled table(s), use the first join key as partition key
      val partitionSources = chain.chainedSources.asScala.zipWithIndex.filter(_._1.partitioning)
      val tryPartitioningInputStream = if (!partitionSources.isEmpty) {
        // use first partition source
        val leftKeyIndex = chain.chainedLeftJoinKeyIndexes.get(partitionSources.head._2)
        new DataStream(
          tableEnv.execEnv,
          PartitionUtils.keyPartition(
            inputDataStream.getTransformation, inputRowTypeInfo, Array(leftKeyIndex)))
      } else {
        inputDataStream
      }

      // using async params from the first table source because they're all same
      val ordered = tableSource.isOrderedMode
      val asyncTimeout = tableSource.getAsyncTimeoutMs
      val asyncBufferCapacity = tableSource.getAsyncBufferCapacity
      val tableNames = hTableSources.asScala.map(_.explainSource()).mkString("|")

      if (ordered) {
        (newInputRowTypeInfo, newInputLen, newRequiredLeftFieldIdx, AsyncDataStream.orderedWait(
          tryPartitioningInputStream,
          asyncFunction,
          asyncTimeout,
          TimeUnit.MILLISECONDS,
          asyncBufferCapacity)
          .name(s"ordered-async-multijoin:$tableNames:"))
      } else { // unordered
        (newInputRowTypeInfo, newInputLen, newRequiredLeftFieldIdx, AsyncDataStream.unorderedWait(
          tryPartitioningInputStream,
          asyncFunction,
          asyncTimeout,
          TimeUnit.MILLISECONDS,
          asyncBufferCapacity)
          .name(s"unordered-async-multijoin:$tableNames:"))
      }
    }

    // divide hTableSources list into several groups that all joins have no transitive dependency
    // in each group.
    var chainNext = true
    var sourceChain = new IntermediateSourceChain
    var currentDataStream: DataStream[BaseRow] = inputDataStream
    var currentInputRowType = inputRowTypeInfo
    var currentInputLen = originalInputLen
    var currentLeftFieldIdx = requiredLeftFieldIdx
    val sourceSize = hTableSources.size()
    for (i <- 0 until sourceSize) {
      if (chainNext) {
        sourceChain.chainedSources.add(hTableSources.get(i))
        sourceChain.chainedJoinTypes.add(joinTypes.get(i))
        sourceChain.chainedLeftJoinKeys.add(leftKeyList.get(i))
        if (hTableSources.get(i).isStrongConsistency) {
          sourceChain.strongConsistencyOnChain = true
        }
      }
      if (i < sourceSize - 1) {
        // not last, then peek next source, check join key dependency
        val nextLeftJoinKey = leftKeyList.get(i + 1)
        // can not chain if next join key belongs to current join's right table
        chainNext = !hTableSources.get(i).getFieldNames.exists(_.equals(nextLeftJoinKey))
        if (!chainNext) {
          // translate current chain
          val newDataStream = translateOneChain(
            currentInputRowType,
            currentInputLen,
            currentLeftFieldIdx,
            currentDataStream,
            sourceChain)
          currentInputRowType = newDataStream._1
          currentInputLen = newDataStream._2
          currentLeftFieldIdx = newDataStream._3
          currentDataStream = newDataStream._4
          //reset chain flag
          chainNext = true
          sourceChain = new IntermediateSourceChain
        }
      } else {
        // reach end, do final translate
        val newDataStream = translateOneChain(
          currentInputRowType,
          currentInputLen,
          currentLeftFieldIdx,
          currentDataStream,
          sourceChain)
        currentDataStream = newDataStream._4
      }
    }
    // final stream
    currentDataStream.getTransformation
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    s"..."
  }

  private def joinTypeToString = {
    joinTypes.asScala.map(
      _ match {
        case JoinRelType.INNER => "InnerJoin"
        case JoinRelType.LEFT => "LeftJoin"
        case _ => "UnexpectedJoinType"
      }).toList.mkString(", ")
  }
}
