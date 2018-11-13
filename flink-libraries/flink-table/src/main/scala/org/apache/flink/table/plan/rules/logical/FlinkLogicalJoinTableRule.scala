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
package org.apache.flink.table.plan.rules.logical

import java.util
import java.util.Comparator

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.util.RexLiteralUtil
import org.apache.flink.table.sources.{DimensionTableSource, IndexKey}

import scala.collection.JavaConverters._
import scala.collection.mutable

class FlinkLogicalJoinTableRule
  extends RelOptRule(
    RelOptRule.operand(
      classOf[FlinkLogicalJoin],
      RelOptRule.operand(classOf[FlinkLogicalRel], RelOptRule.any()),
      RelOptRule.operand(classOf[FlinkLogicalDimensionTableSourceScan], RelOptRule.none())),
    "FlinkLogicalJoinTableRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel[FlinkLogicalJoin](0)
    val joinInfo = join.analyzeCondition()
    val traitSet = join.getTraitSet.replace(FlinkConventions.LOGICAL)
    val relBuilder = call.builder()
    val cluster = join.getCluster

    val left = call.rel[FlinkLogicalRel](1)
    val rightDim = call.rel[FlinkLogicalDimensionTableSourceScan](2)
    val dimCalc = rightDim.calcProgram
    val tableSource = rightDim.tableSource.asInstanceOf[DimensionTableSource[_]]

    val rightIndexes = new util.ArrayList[IndexKey]()
    if (null != tableSource.getIndexes) {
      tableSource.getIndexes.asScala.map(rightIndexes.add(_))
    }
    // sort: unique first
    util.Collections.sort[IndexKey](rightIndexes, new Comparator[IndexKey]() {
      override def compare(o1: IndexKey, o2: IndexKey): Int =
        if (null != o1) o1.compareTo(o2) else 1
    })

    val newJoin = if (dimCalc.isDefined) { // may exist constant lookup key
      val joinRawKeyPairs: util.List[IntPair] = new util.ArrayList[IntPair]()
      joinInfo.pairs().asScala.map {
        p =>
          val calcSrcIdx = getIdenticalSourceField(dimCalc.get, p.target)
          if (calcSrcIdx != -1) {
            joinRawKeyPairs.add(new IntPair(p.source, calcSrcIdx))
          }
      }
      val constantLookupKey = analyzeConstantLookupKeys(join.getCluster, rightDim)
      val lookupKeyCandidates = joinRawKeyPairs.asScala.map(_.target) ++
        constantLookupKey.asScala.keys
      val checkedIndex = checkIndex(lookupKeyCandidates.toArray, rightIndexes)

      val newJoinRemainingCondition: RexNode =
        if (checkedIndex.isDefined && joinInfo.leftKeys.size() >
          (checkedIndex.get.getDefinedColumns.size() - constantLookupKey.size())) {
          splitRemainingCondition(
            joinRawKeyPairs,
            left.getRowType,
            checkedIndex,
            rightDim,
            joinInfo,
            cluster,
            relBuilder)
        } else {
          // join condition may be invalid
          joinInfo.getRemaining(join.getCluster.getRexBuilder)
        }

      val newJoinCondition = if (newJoinRemainingCondition.isAlwaysTrue) {
        None
      } else {
        Some(newJoinRemainingCondition)
      }

      new FlinkLogicalJoinTable(
        join.getCluster,
        traitSet,
        left,
        rightDim.tableSource.asInstanceOf[DimensionTableSource[_]],
        rightDim.calcProgram,
        rightDim.period,
        joinInfo,
        join.getJoinType,
        joinRawKeyPairs,
        newJoinCondition,
        constantLookupKey,
        checkedIndex)

    } else { // without constant lookup key
      /* if (joinInfo.pairs().isEmpty) {
        throw new TableException(
          TableErrors.INST.sqlJoinEqualConditionNotFound("stream to table join"))
      }*/
      val lookupKeyCandidates = joinInfo.pairs().asScala.map(_.target)
      val checkedIndex = checkIndex(lookupKeyCandidates.toArray, rightIndexes)
      val newJoinRemainingCondition: RexNode =
        if (checkedIndex.isDefined &&
          joinInfo.leftKeys.size() > checkedIndex.get.getDefinedColumns.size()) {
          splitRemainingCondition(
            joinInfo.pairs(),
            left.getRowType,
            checkedIndex,
            rightDim,
            joinInfo,
            cluster,
            relBuilder)
        } else {
          // join condition may be invalid
          joinInfo.getRemaining(join.getCluster.getRexBuilder)
        }

      val newJoinCondition = if (newJoinRemainingCondition.isAlwaysTrue) {
        None
      } else {
        Some(newJoinRemainingCondition)
      }

      new FlinkLogicalJoinTable(
        join.getCluster,
        traitSet,
        left,
        rightDim.tableSource.asInstanceOf[DimensionTableSource[_]],
        rightDim.calcProgram,
        rightDim.period,
        joinInfo,
        join.getJoinType,
        joinInfo.pairs(),
        newJoinCondition,
        new util.HashMap(),
        checkedIndex)
    }

    call.transformTo(newJoin)
  }

  private def checkIndex(
      lookupKeyCandidates: Array[Int],
      rightIndexes: util.List[IndexKey]): Option[IndexKey] = {
    // do validation later due to unified ErrorCode
    rightIndexes.asScala.find(_.isIndex(lookupKeyCandidates))
  }

  private def splitRemainingCondition(
      joinRawKeyPairs: util.List[IntPair],
      leftRowType: RelDataType,
      checkedIndex: Option[IndexKey],
      dim: FlinkLogicalDimensionTableSourceScan,
      joinInfo: JoinInfo,
      cluster: RelOptCluster,
      relBuilder: RelBuilder): RexNode = {
    val remainingPairs = if(checkedIndex.isDefined) {
      joinRawKeyPairs.asScala
        .filter(p => !checkedIndex.get.getDefinedColumns.contains(p.target))
    } else {
      joinRawKeyPairs.asScala
    }
    val dimCalc = dim.calcProgram

    //convert remaining pairs to RexInputRef tuple for building sqlStdOperatorTable.EQUALS
    // calls
    val remainingAnds = remainingPairs.map {
      p => {
        val leftInputRef =
          new RexInputRef(p.source, leftRowType.getFieldList.get(p.source).getType)

        val rightInputRef =
          if (dimCalc.isDefined) {
            val calc = dimCalc.get
            // recompute field idx on joinedRow (left + right calc's output)
            val idxInCalc = calc.getOutputRowType.getFieldNames
              .indexOf(calc.getInputRowType.getFieldNames.get(p.target))
            new RexInputRef(
              leftRowType.getFieldCount + idxInCalc,
              calc.getOutputRowType.getFieldList.get(idxInCalc).getType)
          } else {
            new RexInputRef(
              leftRowType.getFieldCount + p.target,
              dim.getRowType.getFieldList.get(p.target).getType)
          }
        (leftInputRef, rightInputRef)
      }
    }
    val equiAnds = relBuilder.and(remainingAnds.map(p => relBuilder.equals(p._1, p._2)): _*)
    relBuilder.and(equiAnds, joinInfo.getRemaining(cluster.getRexBuilder))
  }

  // this is highly inspired by Calcite's RexProgram#getSourceField(int)
  private def getIdenticalSourceField(rexProgram: RexProgram, outputOrdinal: Int): Int = {
    assert((outputOrdinal >= 0) && (outputOrdinal < rexProgram.getProjectList.size()))
    val project = rexProgram.getProjectList.get(outputOrdinal)
    var index = project.getIndex
    while (true) {
      var expr = rexProgram.getExprList.get(index)
      expr match {
        case call: RexCall if call.getOperator == SqlStdOperatorTable.IN_FENNEL =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case call: RexCall if call.getOperator == SqlStdOperatorTable.CAST =>
          // drill through identity function
          expr = call.getOperands.get(0)
        case _ =>
      }
      expr match {
        case ref: RexLocalRef => index = ref.getIndex
        case ref: RexInputRef => return ref.getIndex
        case _ => return -1
      }
    }
    -1
  }

  private def analyzeConstantLookupKeys(
      cluster: RelOptCluster,
      dim: FlinkLogicalDimensionTableSourceScan):
    util.Map[Int, Tuple2[InternalType, Object]] = {

    val constantKeyMap: util.Map[Int, Tuple2[InternalType, Object]] = new util.HashMap
    val calcProgram: Option[RexProgram] = dim.calcProgram
    val indexKeys = mutable.HashSet.empty[Int]
    dim.tableSource.asInstanceOf[DimensionTableSource[_]].getIndexes.asScala
      .map{
        _.getDefinedColumns.asScala.map(indexKeys += _)
      }

    if (calcProgram.isDefined && null != calcProgram.get.getCondition) {
      val condition = RexUtil.toCnf(
        cluster.getRexBuilder,
        calcProgram.get.expandLocalRef(calcProgram.get.getCondition))
      // presume 'A = 1 AND A = 2' will be reduced to ALWAYS_FALSE
      extractConstantKeysFromEquiCondition(condition, indexKeys, constantKeyMap)
    }
    constantKeyMap
  }

  private def extractConstantKeysFromEquiCondition(
      condition: RexNode,
      indexKeys: mutable.Set[Int],
      constantKeyMap: util.Map[Int, Tuple2[InternalType, Object]]): Unit = {

    condition match {
      case c: RexCall if c.getKind == SqlKind.AND =>
        c.getOperands.asScala.foreach(r => extractConstantKeys(r, indexKeys, constantKeyMap))
      case rex: RexNode => extractConstantKeys(rex, indexKeys, constantKeyMap)
      case _ =>
    }
  }

  private def extractConstantKeys(
      pred: RexNode,
      keyIndexes: mutable.Set[Int],
      constantKeyMap: util.Map[Int, Tuple2[InternalType, Object]]): util.Map[Int,
    Tuple2[InternalType, Object]] = {

    pred match {
      case c: RexCall if c.getKind == SqlKind.EQUALS =>
        val leftTerm = c.getOperands.get(0)
        val rightTerm = c.getOperands.get(1)
        val t = FlinkTypeFactory.toInternalType(rightTerm.getType)
        leftTerm match {
          case rexLiteral: RexLiteral =>
            rightTerm match {
              case r: RexInputRef if keyIndexes.contains(r.getIndex) =>
                constantKeyMap.put(
                  r.getIndex,
                  (t, RexLiteralUtil.literalValue(rexLiteral)))
              case _ =>
            }
          case _ => rightTerm match {
            case rexLiteral: RexLiteral =>
              leftTerm match {
                case r: RexInputRef if keyIndexes.contains(r.getIndex) =>
                  constantKeyMap.put(
                    r.getIndex,
                    (t, RexLiteralUtil.literalValue(rexLiteral)))
                case _ =>
              }
            case _ =>
          }
        }
      case _ =>
    }
    constantKeyMap
  }
}

object FlinkLogicalJoinTableRule {
  val INSTANCE = new FlinkLogicalJoinTableRule
}
