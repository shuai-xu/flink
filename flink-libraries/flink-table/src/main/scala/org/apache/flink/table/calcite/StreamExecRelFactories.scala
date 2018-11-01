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

package org.apache.flink.table.calcite

import java.lang.{String => JString}
import java.util
import java.util.function.Supplier
import java.util.{Collections, ArrayList => JArrayList}

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelNode}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.core.RelFactories._
import org.apache.calcite.rel.metadata.RelMdCollation
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SemiJoinType, SqlKind}
import org.apache.calcite.sql.SqlKind.UNION
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkRelFactories.ExpandFactory
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.plan.cost.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.rules.physical.FlinkExpandConversionRule._
import org.apache.flink.table.plan.schema.{BaseRowSchema, DataStreamTable, FlinkRelOptTable, TableSourceTable}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Contains factory interface and default implementation for creating various
  * stream exec rel nodes.
  */
object StreamExecRelFactories {

  val STREAM_EXEC_PROJECT_FACTORY = new ProjectFactoryImpl
  val STREAM_EXEC_FILTER_FACTORY = new FilterFactoryImpl
  val STREAM_EXEC_JOIN_FACTORY = new JoinFactoryImpl
  val STREAM_EXEC_CORRELATE_FACTORY = new CorrelateFactoryImpl
  val STREAM_EXEC_SEMI_JOIN_FACTORY = new SemiJoinFactoryImpl
  val STREAM_EXEC_SORT_FACTORY = new SortFactoryImpl
  val STREAM_EXEC_AGGREGATE_FACTORY = new AggregateFactoryImpl
  val STREAM_EXEC_MATCH_FACTORY = new MatchFactoryImpl
  val STREAM_EXEC_SET_OP_FACTORY = new SetOpFactoryImpl
  val STREAM_EXEC_VALUES_FACTORY = new ValuesFactoryImpl
  val STREAM_EXEC_TABLE_SCAN_FACTORY = new TableScanFactoryImpl
  val STREAM_EXEC_EXPAND_FACTORY = new ExpandFactoryImpl

  /** A [[RelBuilderFactory]] that creates a [[RelBuilder]] that will
    * create logical relational expressions for everything. */
  val STREAM_EXEC_REL_BUILDER: RelBuilderFactory = FlinkRelBuilder.proto(
    Contexts.of(
      STREAM_EXEC_PROJECT_FACTORY,
      STREAM_EXEC_FILTER_FACTORY,
      STREAM_EXEC_JOIN_FACTORY,
      STREAM_EXEC_SEMI_JOIN_FACTORY,
      STREAM_EXEC_SORT_FACTORY,
      STREAM_EXEC_AGGREGATE_FACTORY,
      STREAM_EXEC_MATCH_FACTORY,
      STREAM_EXEC_SET_OP_FACTORY,
      STREAM_EXEC_VALUES_FACTORY,
      STREAM_EXEC_TABLE_SCAN_FACTORY,
      STREAM_EXEC_EXPAND_FACTORY))

  /**
    * Implementation of [[ProjectFactory]] that returns a [[StreamExecCalc]].
    */
  class ProjectFactoryImpl extends ProjectFactory {
    def createProject(
        input: RelNode,
        childExprs: util.List[_ <: RexNode],
        fieldNames: util.List[JString]): RelNode = {
      val cluster = input.getCluster
      val rexBuilder = cluster.getRexBuilder
      val inputRowType = input.getRowType
      val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
      childExprs.zip(fieldNames).foreach { case (childExpr, fieldName) =>
        programBuilder.addProject(childExpr, fieldName)
      }
      val program = programBuilder.getProgram

      val mq = cluster.getMetadataQuery
      val traitSet: RelTraitSet = cluster.traitSet
        .replace(FlinkConventions.STREAMEXEC)
        .replaceIfs(
          RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
            def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, program)
          })

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecCalc to
      // calculate the distribution trait
      val calc = new StreamExecCalc(
        cluster,
        traitSet,
        input,
        program.getOutputRowType,
        program,
        "StreamExecRelFactories"
      )
      val newTraitSet = FlinkRelMetadataQuery.traitSet(calc).simplify()
      calc.copy(newTraitSet, calc.getInputs).asInstanceOf[StreamExecCalc]
    }
  }

  /**
    * Implementation of [[SortFactory]] that returns a [[StreamExecTemporalSort]].
    */
  class SortFactoryImpl extends SortFactory {
    def createSort(
        input: RelNode,
        collation: RelCollation,
        offset: RexNode,
        fetch: RexNode): RelNode = {
      val cluster = input.getCluster
      val collationTrait = RelCollationTraitDef.INSTANCE.canonize(collation)
      val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC).replace(collationTrait)

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecTemporalSort to
      // calculate the distribution trait
      val sort = new StreamExecTemporalSort(
        cluster,
        traitSet,
        input,
        new BaseRowSchema(input.getRowType),
        new BaseRowSchema(input.getRowType),
        collation,
        "StreamExecRelFactories")
      val newTraitSet = FlinkRelMetadataQuery.traitSet(sort).simplify()
      sort.copy(newTraitSet, sort.getInputs).asInstanceOf[StreamExecTemporalSort]
    }

    @deprecated // to be removed before 2.0
    def createSort(
        traits: RelTraitSet, input: RelNode,
        collation: RelCollation, offset: RexNode, fetch: RexNode): RelNode = {
      createSort(input, collation, offset, fetch)
    }
  }

  /**
    * Implementation of [[SetOpFactory]] that
    * returns a [[org.apache.calcite.rel.core.SetOp]] for the particular kind of set
    * operation (UNION, EXCEPT, INTERSECT).
    */
  class SetOpFactoryImpl extends SetOpFactory {
    def createSetOp(kind: SqlKind, inputs: util.List[RelNode], all: Boolean): RelNode = {
      kind match {
        case UNION =>
          val cluster = inputs.get(0).getCluster
          val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)

          // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
          // the distribution trait, so we have to create StreamExecUnion to
          // calculate the distribution trait
          val union = new StreamExecUnion(
            cluster, traitSet, inputs, inputs.get(0).getRowType, all)
          val newTraitSet = FlinkRelMetadataQuery.traitSet(union).simplify()
          union.copy(newTraitSet, union.getInputs).asInstanceOf[StreamExecUnion]

        case _ =>
          throw new AssertionError("not a set op: " + kind)
      }
    }
  }

  /**
    * Implementation of [[AggregateFactory]] that returns a [[StreamExecGroupAggregate]].
    */
  class AggregateFactoryImpl extends AggregateFactory {
    def createAggregate(
        input: RelNode,
        indicator: Boolean,
        groupSet: ImmutableBitSet,
        groupSets: ImmutableList[ImmutableBitSet],
        aggCalls: util.List[AggregateCall]): RelNode = {
      val cluster = input.getCluster
      val outputRowType = Aggregate.deriveRowType(
        cluster.getTypeFactory, input.getRowType, indicator, groupSet, groupSets, aggCalls)

      val requiredDistribution = if (groupSet.cardinality() != 0) {
        FlinkRelDistribution.hash(groupSet.asList)
      } else {
        FlinkRelDistribution.SINGLETON
      }
      val providedTraitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)
      val convInput: RelNode = satisfyDistribution(
        FlinkConventions.STREAMEXEC, input, requiredDistribution)

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecGroupAggregate to
      // calculate the distribution trait
      val aggregate = new StreamExecGroupAggregate(
        cluster,
        providedTraitSet,
        convInput,
        aggCalls,
        outputRowType,
        groupSet)
      val newTraitSet = FlinkRelMetadataQuery.traitSet(aggregate).simplify()
      aggregate.copy(newTraitSet, aggregate.getInputs).asInstanceOf[StreamExecGroupAggregate]
    }
  }

  /**
    * Implementation of [[FilterFactory]] that returns a [[StreamExecCalc]].
    */
  class FilterFactoryImpl extends FilterFactory {
    def createFilter(input: RelNode, condition: RexNode): RelNode = {
      // Create a program containing a filter.
      val cluster = input.getCluster
      val rexBuilder = cluster.getRexBuilder
      val inputRowType = input.getRowType
      val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
      programBuilder.addIdentity()
      programBuilder.addCondition(condition)
      val program = programBuilder.getProgram

      val mq = cluster.getMetadataQuery
      val traitSet: RelTraitSet = cluster.traitSet
        .replace(FlinkConventions.STREAMEXEC)
        .replaceIfs(
          RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
            def get: util.List[RelCollation] = RelMdCollation.calc(mq, input, program)
          })

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecCalc to
      // calculate the distribution trait
      val calc = new StreamExecCalc(
        cluster,
        traitSet,
        input,
        program.getOutputRowType,
        program,
        "StreamExecRelFactories"
      )
      val newTraitSet = FlinkRelMetadataQuery.traitSet(calc).simplify()
      calc.copy(newTraitSet, calc.getInputs).asInstanceOf[StreamExecCalc]
    }
  }

  /**
    * Implementation of [[JoinFactory]] that returns a vanilla
    * [[org.apache.calcite.rel.logical.LogicalJoin]].
    */
  class JoinFactoryImpl extends JoinFactory {
    def createJoin(
        left: RelNode,
        right: RelNode,
        condition: RexNode,
        variablesSet: util.Set[CorrelationId],
        joinType: JoinRelType,
        semiJoinDone: Boolean): RelNode = {
      val cluster = left.getCluster
      val providedTraitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)

      val outputRowType = SqlValidatorUtil.deriveJoinRowType(
        left.getRowType,
        right.getRowType,
        joinType,
        cluster.getTypeFactory,
        null.asInstanceOf[util.List[JString]],
        Collections.emptyList[RelDataTypeField])

      lazy val (joinInfo, filterNulls) = {
        val filterNulls = new util.ArrayList[java.lang.Boolean]
        val joinInfo = JoinInfo.of(left, right, condition, filterNulls)
        (joinInfo, filterNulls.map(_.booleanValue()).toArray)
      }

      def requiredDistribution(
          columns: util.Collection[_ <: Number],
          inputTraitSets: RelTraitSet) = {
        if (columns.size() == 0) {
          FlinkRelDistribution.SINGLETON
        } else {
          FlinkRelDistribution.hash(columns)
        }
      }

      val (leftRequiredDistribution, rightRequiredDistribution) = (
        requiredDistribution(joinInfo.leftKeys, left.getTraitSet),
        requiredDistribution(joinInfo.rightKeys, right.getTraitSet))

      val convLeft: RelNode = satisfyDistribution(
        FlinkConventions.STREAMEXEC, left, leftRequiredDistribution)
      val convRight: RelNode = satisfyDistribution(
        FlinkConventions.STREAMEXEC, right, rightRequiredDistribution)

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecJoin to
      // calculate the distribution trait
      val join = new StreamExecJoin(
        cluster,
        providedTraitSet,
        convLeft,
        convRight,
        outputRowType,
        condition,
        outputRowType,
        joinInfo,
        filterNulls,
        joinInfo.pairs.toList,
        FlinkJoinRelType.toFlinkJoinRelType(joinType),
        null,
        "StreamExecRelFactories")
      val newTraitSet = FlinkRelMetadataQuery.traitSet(join).simplify()
      join.copy(newTraitSet, join.getInputs).asInstanceOf[StreamExecJoin]
    }

    def createJoin(
        left: RelNode,
        right: RelNode,
        condition: RexNode,
        joinType: JoinRelType,
        variablesStopped: util.Set[JString],
        semiJoinDone: Boolean): RelNode = {
      createJoin(
        left,
        right,
        condition,
        CorrelationId.setOf(variablesStopped),
        joinType,
        semiJoinDone)
    }
  }

  /**
    * Implementation of [[CorrelateFactory]] that returns a [[StreamExecCorrelate]].
    */
  class CorrelateFactoryImpl extends CorrelateFactory {
    def createCorrelate(
        left: RelNode,
        right: RelNode,
        correlationId: CorrelationId,
        requiredColumns: ImmutableBitSet,
        joinType: SemiJoinType): RelNode = {
      val cluster = left.getCluster
      val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)

      val outputRowType = joinType match {
        case SemiJoinType.LEFT | SemiJoinType.INNER =>
          SqlValidatorUtil.deriveJoinRowType(
            left.getRowType,
            right.getRowType,
            joinType.toJoinType,
            cluster.getTypeFactory,
            null,
            ImmutableList.of[RelDataTypeField])
        case SemiJoinType.ANTI | SemiJoinType.SEMI =>
          left.getRowType
        case _ =>
          throw new IllegalStateException("Unknown join type " + joinType)
      }

      def getTableScan(calc: Calc): RelNode = {
        val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
        child match {
          case scan: FlinkLogicalTableFunctionScan => scan
          case calc: Calc => getTableScan(calc)
          case _ => throw TableException("This must be a bug, could not find table scan")
        }
      }

      def getMergedCalc(calc: Calc): Calc = {
        val child = calc.getInput.asInstanceOf[RelSubset].getOriginal
        child match {
          case calc1: Calc =>
            val bottomCalc = getMergedCalc(calc1)
            val topCalc = calc
            val topProgram: RexProgram = topCalc.getProgram
            val mergedProgram: RexProgram = RexProgramBuilder
              .mergePrograms(
                topCalc.getProgram,
                bottomCalc.getProgram,
                topCalc.getCluster.getRexBuilder)
            assert(mergedProgram.getOutputRowType eq topProgram.getOutputRowType)
            topCalc.copy(topCalc.getTraitSet, bottomCalc.getInput, mergedProgram)
              .asInstanceOf[Calc]
          case _ =>
            calc
        }
      }

      def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): StreamExecCorrelate = {
        relNode match {
          case rel: RelSubset =>
            convertToCorrelate(rel.getRelList.get(0), condition)

          case calc: Calc => {
            val tableScan = getTableScan(calc)
            val newCalc = getMergedCalc(calc)
            convertToCorrelate(
              tableScan,
              Some(newCalc.getProgram.expandLocalRef(newCalc.getProgram.getCondition)))
          }

          case scan: FlinkLogicalTableFunctionScan =>
            new StreamExecCorrelate(
              cluster,
              traitSet,
              left,
              None,
              scan,
              condition,
              outputRowType,
              joinType,
              "StreamExecRelFactories")
        }
      }

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecCorrelate to
      // calculate the distribution trait
      val correlate = convertToCorrelate(right, None)
      val newTraitSet = FlinkRelMetadataQuery.traitSet(correlate).simplify()
      correlate.copy(newTraitSet, correlate.getInputs).asInstanceOf[StreamExecCorrelate]
    }
  }

  /**
    * Implementation of [[SemiJoinFactory} that returns a [[StreamExecJoin]].
    */
  class SemiJoinFactoryImpl extends SemiJoinFactory {
    def createSemiJoin(left: RelNode, right: RelNode, condition: RexNode): RelNode = {
      createJoin(left, right, condition, isAntiJoin = false)
    }

    def createAntiJoin(left: RelNode, right: RelNode, condition: RexNode): RelNode = {
      val joinInfo: JoinInfo = JoinInfo.of(left, right, condition)
      createJoin(left, right, condition, isAntiJoin = true)
    }

    private def createJoin(
        left: RelNode, right: RelNode, condition: RexNode, isAntiJoin: Boolean): RelNode = {
      val cluster = left.getCluster
      val providedTraitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)
      val outputRowType = SqlValidatorUtil.deriveJoinRowType(
        left.getRowType,
        null.asInstanceOf[RelDataType],
        JoinRelType.INNER,
        cluster.getTypeFactory,
        null,
        Collections.emptyList[RelDataTypeField])

      val joinRowType = SqlValidatorUtil.deriveJoinRowType(
        left.getRowType,
        right.getRowType,
        JoinRelType.INNER,
        cluster.getTypeFactory,
        null,
        Collections.emptyList[RelDataTypeField])

      lazy val (joinInfo, filterNulls) = {
        val filterNulls = new util.ArrayList[java.lang.Boolean]
        val joinInfo = JoinInfo.of(left, right, condition, filterNulls)
        (joinInfo, filterNulls.map(_.booleanValue()).toArray)
      }

      def toHashTraitByColumns(
          columns: util.Collection[_ <: Number],
          inputTraitSet: RelTraitSet) = {
        val distribution = if (columns.size() == 0) {
          FlinkRelDistribution.SINGLETON
        } else {
          FlinkRelDistribution.hash(columns)
        }
        inputTraitSet.replace(distribution)
      }

      val (leftRequiredTrait, rightRequiredTrait) = (
        toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
        toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))

      val convLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
      val convRight: RelNode = RelOptRule.convert(right, rightRequiredTrait)
      val joinType = if (isAntiJoin) {
        FlinkJoinRelType.ANTI
      } else {
        FlinkJoinRelType.SEMI
      }

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecJoin to
      // calculate the distribution trait
      val semiJoin = new StreamExecJoin(
        cluster,
        providedTraitSet,
        convLeft,
        convRight,
        outputRowType,
        condition,
        joinRowType,
        joinInfo,
        filterNulls,
        joinInfo.pairs().asScala.toList,
        joinType,
        null,
        "StreamExecRelFactories")
      val newTraitSet = FlinkRelMetadataQuery.traitSet(semiJoin).simplify()
      semiJoin.copy(newTraitSet, semiJoin.getInputs).asInstanceOf[StreamExecJoin]
    }
  }

  /**
    * Implementation of [[ValuesFactory]] that returns a [[StreamExecValues]].
    */
  class ValuesFactoryImpl extends ValuesFactory {
    def createValues(
        cluster: RelOptCluster,
        rowType: RelDataType,
        tuples: util.List[ImmutableList[RexLiteral]]): RelNode = {
      val immutableTuples = ImmutableList.copyOf[ImmutableList[RexLiteral]](tuples)
      val mq = cluster.getMetadataQuery
      val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC).replaceIfs(
        RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
          def get: util.List[RelCollation] = RelMdCollation.values(mq, rowType, immutableTuples)
        })
      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecValues to
      // calculate the distribution trait
      val values = new StreamExecValues(
        cluster, traitSet, new BaseRowSchema(rowType), immutableTuples, "StreamExecRelFactories")
      val newTraitSet = FlinkRelMetadataQuery.traitSet(values).simplify()
      values.copy(newTraitSet, values.getInputs).asInstanceOf[StreamExecValues]
    }
  }

  /**
    * Implementation of [[TableScanFactory]] that returns a
    * [[StreamExecTableSourceScan]] or [[StreamExecScan]].
    */
  class TableScanFactoryImpl extends TableScanFactory {
    def createScan(cluster: RelOptCluster, relOptTable: RelOptTable): RelNode = {
      relOptTable.unwrap(classOf[TableSourceTable]) match {
        case tst: TableSourceTable =>
          val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC).replaceIfs(
            RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
              def get: util.List[RelCollation] = {
                if (tst != null) {
                  tst.getStatistic.getCollations
                } else {
                  ImmutableList.of[RelCollation]
                }
              }
            })
          // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
          // the distribution trait, so we have to create StreamExecTableSourceScan to
          // calculate the distribution trait
          val scan = new StreamExecTableSourceScan(
            cluster, traitSet, relOptTable.asInstanceOf[FlinkRelOptTable])
          val newTraitSet = FlinkRelMetadataQuery.traitSet(scan).simplify()
          scan.copy(newTraitSet, scan.getInputs).asInstanceOf[StreamExecTableSourceScan]

        case _ =>
          val dst = relOptTable.unwrap(classOf[DataStreamTable[_]])
          val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC).replaceIfs(
            RelCollationTraitDef.INSTANCE, new Supplier[util.List[RelCollation]]() {
              def get: util.List[RelCollation] = {
                if (dst != null) {
                  dst.getStatistic.getCollations
                } else {
                  ImmutableList.of[RelCollation]
                }
              }
            })
          // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
          // the distribution trait, so we have to create StreamExecScan to
          // calculate the distribution trait
          val scan = new StreamExecScan(
            cluster, traitSet, relOptTable, dst.getRowType(cluster.getTypeFactory))
          val newTraitSet = FlinkRelMetadataQuery.traitSet(scan).simplify()
          scan.copy(newTraitSet, scan.getInputs).asInstanceOf[StreamExecScan]
      }
    }
  }

  /**
    * Implementation of [[MatchFactory]] that returns a [[StreamExecMatch]].
    */
  class MatchFactoryImpl extends MatchFactory {
    def createMatch(
        input: RelNode,
        pattern: RexNode,
        rowType: RelDataType,
        strictStart: Boolean,
        strictEnd: Boolean,
        patternDefinitions: util.Map[JString, RexNode],
        measures: util.Map[JString, RexNode],
        after: RexNode,
        subsets: util.Map[JString, _ <: util.SortedSet[JString]],
        rowsPerMatch: RexNode,
        partitionKeys: util.List[RexNode],
        orderKeys: RelCollation,
        interval: RexNode,
        emit: RexNode): RelNode = {
      val cluster = input.getCluster

      val logicalKeys = partitionKeys.asScala.map {
        case inputRef: RexInputRef => inputRef.getIndex
      }

      val distributionFields: JArrayList[Integer] = {
        val fields = new JArrayList[Integer]()
        for (field <- logicalKeys.toArray) fields.add(field)
        fields
      }

      val requiredDistribution = if (logicalKeys.nonEmpty) {
        FlinkRelDistribution.hash(distributionFields)
      } else {
        FlinkRelDistribution.ANY
      }
      val requiredTraitSet = input.getTraitSet.replace(requiredDistribution)
      val providedTraitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)

      val convertInput: RelNode = RelOptRule.convert(input, requiredTraitSet)
      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecMatch to
      // calculate the distribution trait
      val streamExecMatch = new StreamExecMatch(
        cluster,
        providedTraitSet,
        convertInput,
        pattern,
        strictStart,
        strictEnd,
        patternDefinitions,
        measures,
        after,
        subsets,
        rowsPerMatch,
        partitionKeys,
        orderKeys,
        interval,
        emit,
        new BaseRowSchema(rowType),
        new BaseRowSchema(convertInput.getRowType))
      val newTraitSet = FlinkRelMetadataQuery.traitSet(streamExecMatch).simplify()
      streamExecMatch.copy(newTraitSet, streamExecMatch.getInputs).asInstanceOf[StreamExecMatch]
    }
  }

  /**
    * Implementation of [[FlinkRelFactories#ExpandFactory]] that returns a
    * [[org.apache.flink.table.plan.nodes.physical.stream.StreamExecExpand]].
    */
  class ExpandFactoryImpl extends ExpandFactory {
    def createExpand(
        input: RelNode,
        rowType: RelDataType,
        projects: util.List[util.List[RexNode]],
        expandIdIndex: Int): RelNode = {

      val cluster = input.getCluster
      val traitSet = cluster.traitSetOf(FlinkConventions.STREAMEXEC)
      val convInput: RelNode = RelOptRule.convert(input, FlinkConventions.STREAMEXEC)

      // FIXME: FlinkRelMdDistribution requires the current RelNode to compute
      // the distribution trait, so we have to create StreamExecExpand to
      // calculate the distribution trait
      val expand = new StreamExecExpand(
        cluster,
        traitSet,
        convInput,
        rowType,
        projects,
        expandIdIndex,
        "StreamExecRelFactories")
      val newTraitSet = FlinkRelMetadataQuery.traitSet(expand).simplify()
      expand.copy(newTraitSet, expand.getInputs).asInstanceOf[StreamExecExpand]
    }
  }
}
