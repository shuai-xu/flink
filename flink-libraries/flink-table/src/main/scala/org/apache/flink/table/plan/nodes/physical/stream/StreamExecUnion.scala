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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.streaming.api.transformations.{StreamTransformation, UnionTransformation}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Union.
  *
  */
class StreamExecUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    outputRowType: RelDataType)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with StreamExecRel {

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecUnion(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      outputRowType
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union all", outputRowType.getFieldNames.mkString(", "))
  }

  override def toString: String = {
    s"Union All(union: (${outputRowType.getFieldNames.mkString(", ")}))"
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): StreamTransformation[BaseRow] = {
    val leftFields = leftNode.getRowType.getFieldList.asScala.map(
      t => (t.getName, FlinkTypeFactory.toTypeInfo(t.getType)))
    val rightFields = rightNode.getRowType.getFieldList.asScala.map(
      t => (t.getName, FlinkTypeFactory.toTypeInfo(t.getType))
    )

    if(leftFields.length != rightFields.length) {
      throw new IllegalArgumentException(
        TableErrors.INST.sqlUnionAllFieldsLenMismatch(
          leftFields.map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]"),
          rightFields.map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]")))
    } else if(!leftFields.map { case (n, t) => t }.equals(rightFields.map { case (n, t) => t })) {
      val diffFields = leftFields.zip(rightFields).filter{
        case ((_, type1), (_, type2)) => type1 != type2 }
      throw new IllegalArgumentException(
        TableErrors.INST.sqlUnionAllFieldsTypeMismatch(
          diffFields.map(_._1).map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]"),
          diffFields.map(_._2).map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]")))
    }

    val leftInput = getLeft.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    val rightInput = getRight.asInstanceOf[StreamExecRel].translateToPlan(tableEnv, queryConfig)
    new UnionTransformation(Array(leftInput, rightInput).toList)
  }
}
