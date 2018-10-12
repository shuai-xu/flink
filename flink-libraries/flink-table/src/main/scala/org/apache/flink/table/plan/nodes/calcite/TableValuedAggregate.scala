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

package org.apache.flink.table.plan.nodes.calcite

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.functions.utils.TableValuedAggSqlFunction
import org.apache.flink.table.types.DataType

import scala.collection.JavaConversions._

/**
  * Common table-valued aggregate relNode.
  */
abstract class TableValuedAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    resultType: DataType,
    groupKey: Seq[RexNode],
    groupKeyNames: Seq[String])
  extends SingleRel(cluster, traitSet, input)
  with CommonTableValuedAgg {

  private[flink] def buildAggregationToString(
    inputType: RelDataType,
    sqlFunction:TableValuedAggSqlFunction,
    argsIndex:Array[Int]): String = {
    val inFields = inputType.getFieldNames
    s"${sqlFunction.getFunction.toString}(${argsIndex.map(inFields(_)).mkString(", ")})"
  }

  override def deriveRowType: RelDataType = {
    getRowType(cluster, resultType, groupKey, groupKeyNames)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupKey", groupKey.map(_.toString).mkString(", "), groupKey.nonEmpty)
      .item("select", getRowType.getFieldNames.mkString(", "))
  }
}
