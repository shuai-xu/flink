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

import org.apache.flink.streaming.api.transformations.{StreamTransformation, UnionTransformation}
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{SetOp, Union}
import org.apache.calcite.rel.{RelNode, RelWriter}

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Union.
  *
  */
class StreamExecUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relList: JList[RelNode],
    outputRowType: RelDataType,
    all: Boolean)
  extends Union(cluster, traitSet, relList, all)
  with RowStreamExecRel {

  require(all, "Only support union all")

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: JList[RelNode], all: Boolean): SetOp = {
    new StreamExecUnion(
      cluster,
      traitSet,
      inputs,
      outputRowType,
      all
    )
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("union", outputRowType.getFieldNames.mkString(", "))
  }

  override def isDeterministic: Boolean = true

  override def translateToPlan(tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val inputs = getInputs
    val firstInputRowType = inputs.head.getRowType
    val firstInputFields = firstInputRowType.getFieldList.asScala.map(
      t => (t.getName, FlinkTypeFactory.toTypeInfo(t.getType)))
    val firstInputFieldsCnt = firstInputRowType.getFieldCount
    val fieldsCntMismatchInputs = inputs.drop(1).filter(r =>
      r.getRowType.getFieldCount != firstInputFieldsCnt)
    if (fieldsCntMismatchInputs.nonEmpty) {
      val mismatchFields = fieldsCntMismatchInputs.head.getRowType.getFieldList.asScala.map(
        t => (t.getName, FlinkTypeFactory.toTypeInfo(t.getType))
      )
      throw new IllegalArgumentException(
        TableErrors.INST.sqlUnionAllFieldsCntMismatch(
          firstInputFields.map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]"),
          mismatchFields.map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]")))
    }

    val fieldsTypeMismatchInputs = inputs.drop(1).filter(r =>
      !FlinkTypeFactory.toTypeInfo(r.getRowType).equals(
        FlinkTypeFactory.toTypeInfo(firstInputRowType)))
    if (fieldsTypeMismatchInputs.nonEmpty) {
      val mismatchFields = fieldsTypeMismatchInputs.head.getRowType.getFieldList.asScala.map(
        t => (t.getName, FlinkTypeFactory.toTypeInfo(t.getType))
      )
      val diffFields = firstInputFields.zip(mismatchFields).filter {
        case ((_, type1), (_, type2)) => type1 != type2
      }
      throw new IllegalArgumentException(
        TableErrors.INST.sqlUnionAllFieldsTypeMismatch(
          diffFields.map(_._1).map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]"),
          diffFields.map(_._2).map { case (n, t) => s"$n:$t" }.mkString("[", ", ", "]")))
    }

    val transformations = getInputs.map(_.asInstanceOf[RowStreamExecRel].translateToPlan(tableEnv))
    val outputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType, classOf[BinaryRow])
    new UnionTransformation(transformations, outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
  }
}
