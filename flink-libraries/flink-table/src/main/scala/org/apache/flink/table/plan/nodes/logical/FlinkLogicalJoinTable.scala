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
package org.apache.flink.table.plan.nodes.logical

import java.util
import java.util.Collections

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex._
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.table.api.types.{BaseRowType, InternalType}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.cost.FlinkRelMdSize
import org.apache.flink.table.plan.nodes.common.CommonCalc
import org.apache.flink.table.plan.schema.{BaseRowSchema, TimeIndicatorRelDataType}
import org.apache.flink.table.sources.{DimensionTableSource, IndexKey}

import scala.collection.JavaConverters._

class FlinkLogicalJoinTable(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val tableSource: DimensionTableSource[_],
    val calcProgram: Option[RexProgram],
    val period: Option[RexNode],
    val joinInfo: JoinInfo,
    val joinType: JoinRelType,
    val joinRawKeyPairs: util.List[IntPair],
    val newJoinRemainingCondition: Option[RexNode],
    val constantLookupKeys: util.Map[Int, Tuple2[InternalType, Object]],
    val checkedIndex: Option[IndexKey])
  extends SingleRel(cluster, traitSet, input)
  with FlinkLogicalRel
  with CommonCalc {

  // TODO remove this exception when support scannable dim table
  if (checkedIndex.isEmpty) {
    Litmus.THROW.fail(TableErrors.INST.sqlDimJoinRequireEqCondOnIndex())
  }

  val checkedLookupKeys: Array[Int] = checkedIndex.get.toArray
  val lookupKeyPairs: List[IntPair] = joinRawKeyPairs.asScala.filter {
    p => checkedLookupKeys.contains(p.target)
  }.toList

  // do not rely on Calcite's call which needs assertion enabled
  validate(Litmus.THROW, null)

  // Calcite's isValid() relies assertion enabled and will validate twice for normal case since
  // we want this is a force validation even if assertion is disabled.
  def validate(litmus: Litmus, context: RelNode.Context): Boolean = {
    if (joinRawKeyPairs.isEmpty && constantLookupKeys.isEmpty) {
      return litmus.fail(TableErrors.INST.sqlDimJoinRequireEqCondOnIndex())
    }

    val indexes = tableSource.getIndexes
    // checked index never be null, so declared index also not null.
    if (indexes.isEmpty) {
      // TODO remove this exception when support scannable dim table
      return litmus.fail(TableErrors.INST.sqlDimTableRequiresIndex())
    }

    def getTableSourceSchema(): BaseRowSchema = {
      val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
      val tableSourceRowType = flinkTypeFactory.buildLogicalRowType(
        tableSource.getTableSchema, false)
      new BaseRowSchema(tableSourceRowType)
    }

    val leftKeys = lookupKeyPairs.map(_.source).toArray
    val rightKeys = lookupKeyPairs.map(_.target) ++ constantLookupKeys.asScala.keys
    val inputSchema = new BaseRowSchema(input.getRowType)
    val tableSourceSchema = getTableSourceSchema()
    val leftKeyTypes = leftKeys.map(inputSchema.fieldTypeInfos(_))
    // use original keyPair to validate key types (rigthKeys may include constant keys)
    val rightKeyTypes = lookupKeyPairs.map(p => tableSourceSchema.fieldTypeInfos(p.target))

    // check type
    leftKeyTypes.zip(rightKeyTypes).foreach(f => {
      if (f._1 != f._2) {
        val leftNames = leftKeys.map(inputSchema.fieldNames(_))
        val rightNames = rightKeys.map(tableSourceSchema.fieldNames(_))

        val leftNameTypes = leftKeyTypes
          .zip(leftNames)
          .map(f => s"${f._2}[${f._1.toString}]")

        val rightNameTypes = rightKeyTypes
          .zip(rightNames)
          .map(f => s"${f._2}[${f._1.toString}]")

        val condition = leftNameTypes
          .zip(rightNameTypes)
          .map(f => s"${f._1}=${f._2}")
          .mkString(", ")
        return litmus.fail(TableErrors.INST.sqlJoinEqualJoinOnIncompatibleTypes(
          s"And the condition is $condition"))
      }
    })

    if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
      return litmus.fail(
        TableErrors.INST.sqlJoinTypeNotSupported("stream to table join",
          joinType.toString))
    }

    val tableReturnType = tableSource.getReturnType
    if (!tableReturnType.isInstanceOf[BaseRowType]) {
      return litmus.fail(
        TableErrors.INST.sqlStreamToTblJoinDimTypeNotSupported(tableReturnType.toString))
    }

    if (tableSource.isTemporal) {
      period match {
        case Some(p) =>
          p.getType match {
            case t: TimeIndicatorRelDataType if !t.isEventTime => // ok
            case _ =>
              return litmus.fail(TableErrors.INST.sqlJoinTemporalTableError(
                "Currently only support join temporal table as of on left table's proctime field"))
          }
          p match {
            case r: RexFieldAccess if r.getReferenceExpr.isInstanceOf[RexCorrelVariable] =>
            // it's left table's field, ok
            case call: RexCall if call.getOperator == ScalarSqlFunctions.PROCTIME =>
            // it is PROCTIME() call, ok
            case _ =>
              return litmus.fail(TableErrors.INST.sqlJoinTemporalTableError(
                "Currently only support join temporal table as of on left table's proctime field."))
          }
        case None =>
          return litmus.fail(TableErrors.INST.sqlJoinTemporalTableError(
            "Join temporal table must follow a period specification: FOR SYSTEM_TIME AS OF"))
      }
    } else {
      if (period.isDefined) {
        return litmus.fail(TableErrors.INST.sqlJoinInternalError(
          "Join a static table shouldn't follow a period specification. This should never happen."))
      }
    }
    litmus.succeed()
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalJoinTable(
      cluster,
      traitSet,
      inputs.get(0),
      tableSource,
      calcProgram,
      period,
      joinInfo,
      joinType,
      joinRawKeyPairs,
      newJoinRemainingCondition,
      constantLookupKeys,
      checkedIndex)
  }

  override def deriveRowType(): RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    SqlValidatorUtil.deriveJoinRowType(
      input.getRowType,
      getRightType,
      joinType,
      flinkTypeFactory,
      null,
      Collections.emptyList[RelDataTypeField])
  }

  private def getRightType: RelDataType = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    if (calcProgram.isDefined) {
      calcProgram.get.getOutputRowType
    } else {
      flinkTypeFactory.buildLogicalRowType(tableSource.getTableSchema, false)
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rightRowCnt = 1.0
    val rightRowSize = FlinkRelMdSize.averageTypeValueSize(getRightType)
    val additionalCost = planner.getCostFactory.makeCost(rightRowCnt, rightRowCnt * rightRowSize, 0)

    // add some additional cost
    input.computeSelfCost(planner, mq).plus(additionalCost)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val desc = if (tableSource.explainSource().isEmpty) {
      tableSource.getClass.getSimpleName
    } else {
      tableSource.explainSource()
    }
    val remaining = newJoinRemainingCondition
    pw
      .item("input", getInput)
      .item("dimTable", desc)
      .item("on", joinRawKeyPairs)
      .itemIf(
        "constantLookupKey",
        constantLookupKeys.asScala.map(k => "[" + k._1 + "] = " + k._2),
        !constantLookupKeys.isEmpty)
    if (remaining.isDefined && !remaining.get.isAlwaysTrue) {
      pw.item("joinCondition", remaining.get)
    }
    pw.item("joinType", joinType.lowerName)
    if (calcProgram.isDefined) {
      pw
        .item("dimProject", selectionToString(calcProgram.get, getExpressionString))
        .itemIf(
          "dimFilter",
          conditionToString(calcProgram.get, getExpressionString),
          calcProgram.get.getCondition != null)
    }

    pw
  }
}
