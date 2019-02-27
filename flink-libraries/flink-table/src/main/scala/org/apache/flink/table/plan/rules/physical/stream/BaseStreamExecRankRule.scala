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

package org.apache.flink.table.plan.rules.physical.stream

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalRank
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecFirstLastRow
import org.apache.flink.table.plan.util.ConstantRankRange

import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.SqlKind

trait BaseStreamExecRankRule {

  /**
    * Whether the input rank could be simplified to [[StreamExecFirstLastRow]].
    * The [[FlinkLogicalRank]] which sorts on time attribute and limits 1 and RankFunction
    * is ROW_NUMBER could be simplified.
    *
    * @param rank The [[FlinkLogicalRank]] node
    * @return True if the input rank could be simplified to [[StreamExecFirstLastRow]]
    */
  def canSimplifyToFirstLastRow(rank: FlinkLogicalRank): Boolean = {
    val sortCollation = rank.sortCollation
    val rankRange = rank.rankRange

    val isRowNumberFunction = rank.rankFunction.getKind == SqlKind.ROW_NUMBER

    val limit1 = rankRange match {
      case ConstantRankRange(rankStart, rankEnd) => rankStart == 1 && rankEnd == 1
      case _ => false
    }

    val inputRowType = rank.getInput.getRowType
    !rank.outputRankFunColumn && limit1 && sortOnProcTimeAttribute(sortCollation, inputRowType) &&
      isRowNumberFunction
  }

  /**
    * whether the input sort could be simplified to [[StreamExecFirstLastRow]].
    * The [[Sort]] which sorts on time attribute and fetch first record start with 0 could be
    * simplified.
    *
    * @param sort the [[Sort]] node
    * @return True if the input sort could be simplified to [[StreamExecFirstLastRow]]
    */
  def canSimplifyToFirstLastRow(sort: Sort): Boolean = {
    val sortCollation = sort.collation
    val inputRowType = sort.getInput.getRowType

    sortOnProcTimeAttribute(sortCollation, inputRowType) &&
      (sort.offset == null || RexLiteral.intValue(sort.offset) == 0) &&
      (sort.fetch != null && RexLiteral.intValue(sort.fetch) == 1)
  }

  private def sortOnProcTimeAttribute(
    sortCollation: RelCollation,
    inputRowType: RelDataType): Boolean = {
    if (sortCollation.getFieldCollations.size() != 1) {
      false
    } else {
      val fieldCollation = sortCollation.getFieldCollations.get(0)
      val fieldType = inputRowType.getFieldList.get(fieldCollation.getFieldIndex).getType
      FlinkTypeFactory.isProctimeIndicatorType(fieldType)
    }
  }

}
