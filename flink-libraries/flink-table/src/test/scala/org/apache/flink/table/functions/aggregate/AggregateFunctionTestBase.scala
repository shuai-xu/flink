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
package org.apache.flink.table.functions.aggregate

import java.util.function.{Function => JFunction}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{BigDecimalTypeInfo, TypeInformation}
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.jobgraph.OperatorID
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.operators.StreamOperator
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.{OneInputStreamTask, OneInputStreamTaskTestHarness, StreamTask}
import org.apache.flink.table.api._
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkRelOptClusterFactory, FlinkTypeFactory}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.expressions.{Alias, Expression}
import org.apache.flink.table.functions.UserDefinedFunction
import org.apache.flink.table.plan.logical.Aggregate
import org.apache.flink.table.plan.nodes.physical.batch.{BatchExecLocalSortAggregate, BatchExecSortAggregate}
import org.apache.flink.table.plan.util.AggregateUtil
import org.apache.flink.table.dataformat._
import org.apache.flink.table.runtime.operator.OneInputSubstituteStreamOperator
import org.apache.flink.table.types.{BaseRowType, DataTypes}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.mockito.Mockito.{mock, when}

import _root_.scala.collection.JavaConversions._
import _root_.scala.util.Random

/**
 * We use SortAggregate to test all aggregate functions. Since the testing is aiming at aggregate
 * function's internal logic, so there will be no grouping keys.
 */
abstract class AggregateFunctionTestBase {

  def testAggregateFunctions(
      inputData: Seq[BinaryRow],
      inputDataType: BaseRowTypeInfo[_],
      aggExprs: Seq[Expression],
      localResultType: BaseRowTypeInfo[_],
      expectedFinalResult: BinaryRow,
      expectedFinalResultType: BaseRowTypeInfo[_]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig())

    // create a mocked input table
    val bs = mock(classOf[DataStream[Any]])
    val transform = mock(classOf[StreamTransformation[Any]])
    when(bs.getTransformation).thenReturn(transform)
    val returnType = inputDataType.asInstanceOf[TypeInformation[Any]]
    when(transform.getOutputType).thenReturn(returnType)
    when(bs.getType).thenReturn(returnType)
    tableEnv.registerBoundedStream("mockTable", bs)

    verifyAggregateFunctions(
      tableEnv,
      inputData,
      inputDataType,
      aggExprs,
      localResultType,
      expectedFinalResult,
      expectedFinalResultType
    )
  }

  def testWithFixLengthString(
      inputData: Seq[BinaryRow],
      inputDataType: BaseRowTypeInfo[_],
      aggExprs: Seq[Expression],
      localResultType: BaseRowTypeInfo[_],
      expectedFinalResult: BinaryRow,
      expectedFinalResultType: BaseRowTypeInfo[_]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig())

    // create a table with CHAR(5) field
    val sqlQuery = "VALUES ('TEST'), ('HELLO'), ('WORLD')"
    val table = tableEnv.sqlQuery(sqlQuery).as("f0")
    tableEnv.registerTable("mockTable", table)

    verifyAggregateFunctions(
      tableEnv,
      inputData,
      inputDataType,
      aggExprs,
      localResultType,
      expectedFinalResult,
      expectedFinalResultType
    )
  }

  private[flink] def row(tpe: BaseRowTypeInfo[_], fields: Any*): BinaryRow = {
    assertEquals(
      "Filed count inconsistent with type information",
      fields.length,
      tpe.getFieldNames.length)
    val row = new BinaryRow(fields.length)
    val writer = new BinaryRowWriter(row)
    writer.reset()
    fields.zipWithIndex.foreach { case (field, index) =>
      val typeInfo = tpe.getTypeAt(index)
      if (field == null) writer.setNullAt(index)
      else writer.write(
        index,
        if (typeInfo == Types.STRING) {
          BinaryString.fromString(field.asInstanceOf[String])
        } else if (typeInfo.isInstanceOf[BigDecimalTypeInfo]) {
          val dt = typeInfo.asInstanceOf[BigDecimalTypeInfo]
          Decimal.fromBigDecimal(field.asInstanceOf[_root_.java.math.BigDecimal],
            dt.precision(), dt.scale())
        } else {
          field
        },
        typeInfo,
        typeInfo.createSerializer(null))
    }
    writer.complete()
    row
  }

  private def verifyAggregateFunctions(
      tableEnv: BatchTableEnvironment,
      inputData: Seq[BinaryRow],
      inputDataType: BaseRowTypeInfo[_],
      aggExprs: Seq[Expression],
      localResultType: BaseRowTypeInfo[_],
      expectedFinalResult: BinaryRow,
      expectedFinalResultType: BaseRowTypeInfo[_]): Unit = {
    // convert to logical aggregate plan
    val builder = FlinkRelBuilder.create(tableEnv.getFrameworkConfig, tableEnv.getConfig)
    val plan = Aggregate(
      Nil,
      aggExprs.zipWithIndex.map { case (agg, index) => Alias(agg, "agg" + index) },
      tableEnv.scan("mockTable").logicalPlan)

    val logicalAggregate = plan.validate(tableEnv).toRelNode(builder).asInstanceOf[LogicalAggregate]
    val aggCalls = logicalAggregate.getAggCallList.toSeq
    val inputRelDataType = logicalAggregate.getInput.getRowType
    val outputRelDataType = logicalAggregate.getRowType
    val (_, _, aggFunctions) = AggregateUtil.transformToBatchAggregateFunctions(
      aggCalls,
      inputRelDataType)
    val aggCallToAggFunction = aggCalls.zip(aggFunctions)

    verifySinglePhaseAggregation(
      tableEnv,
      builder,
      aggCallToAggFunction,
      inputRelDataType,
      outputRelDataType,
      inputData,
      inputDataType,
      expectedFinalResult,
      expectedFinalResultType)

    verifyTwoPhaseAggregation(
      tableEnv,
      builder,
      aggCallToAggFunction,
      inputRelDataType,
      outputRelDataType,
      inputData,
      inputDataType,
      localResultType,
      expectedFinalResult,
      expectedFinalResultType)
  }

  private def verifySinglePhaseAggregation(
      tableEnv: BatchTableEnvironment,
      builder: FlinkRelBuilder,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      inputRelDataType: RelDataType,
      outputRelDataType: RelDataType,
      inputData: Seq[BinaryRow],
      inputDataType: BaseRowTypeInfo[_],
      expectedFinalResult: BinaryRow,
      expectedFinalResultType: BaseRowTypeInfo[_]): Unit = {
    val agg = new BatchExecSortAggregate(
      cluster = FlinkRelOptClusterFactory.create(tableEnv.getPlanner, builder.getRexBuilder),
      relBuilder = tableEnv.getRelBuilder,
      traitSet = null,
      inputNode = null,
      aggCallToAggFunction = aggCallToAggFunction,
      rowRelDataType = outputRelDataType,
      inputRelDataType = inputRelDataType,
      grouping = Array(),
      auxGrouping = Array(),
      isMerge = false)
    val outputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(
      outputRelDataType, classOf[GenericRow])
    val ctx = CodeGeneratorContext(tableEnv.getConfig, true)
    val generatedOperator = agg.codegenWithoutKeys(
      isMerge = false,
      isFinal = true,
      ctx,
      tableEnv,
      DataTypes.internal(inputDataType).asInstanceOf[BaseRowType],
      DataTypes.internal(outputRowType).asInstanceOf[BaseRowType],
      "Sort")
    val operator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedOperator.name,
      generatedOperator.code,
      references = ctx.references)

    checkOperatorResult(
      operator,
      inputData,
      inputDataType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      Seq(expectedFinalResult),
      expectedFinalResultType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      outputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
  }

  private def verifyTwoPhaseAggregation(
      tableEnv: BatchTableEnvironment,
      builder: FlinkRelBuilder,
      aggCallToAggFunction: Seq[(AggregateCall, UserDefinedFunction)],
      inputRelDataType: RelDataType,
      outputRelDataType: RelDataType,
      inputData: Seq[BinaryRow],
      inputDataType: BaseRowTypeInfo[_],
      localResultType: BaseRowTypeInfo[_],
      expectedFinalResult: BinaryRow,
      expectedFinalResultType: BaseRowTypeInfo[_]): Unit = {
    val localOutputRelDataType = builder.getTypeFactory.buildLogicalRowType(
      localResultType.getFieldNames,
      localResultType.getFieldTypes)

    val localAgg = new BatchExecLocalSortAggregate(
      cluster = FlinkRelOptClusterFactory.create(tableEnv.getPlanner, builder.getRexBuilder),
      relBuilder = tableEnv.getRelBuilder,
      traitSet = null,
      inputNode = null,
      aggCallToAggFunction = aggCallToAggFunction,
      rowRelDataType = localOutputRelDataType,
      inputRelDataType = inputRelDataType,
      grouping = Array(),
      auxGrouping = Array())
    val localOutputRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(
      localOutputRelDataType, classOf[GenericRow])
    val config = tableEnv.getConfig
    val localOperator = {
      val ctx = CodeGeneratorContext(config, true)
      val generatedLocalOperator = localAgg.codegenWithoutKeys(
        isMerge = false,
        isFinal = false,
        ctx,
        tableEnv,
        DataTypes.internal(inputDataType).asInstanceOf[BaseRowType],
        DataTypes.internal(localOutputRowType).asInstanceOf[BaseRowType],
        "Sort")
      new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
        generatedLocalOperator.name,
        generatedLocalOperator.code,
        references = ctx.references)
    }

    val (localInput1, localInput2) = Random.shuffle(inputData).splitAt(inputData.length / 2)
    val localOutput1 = getLocalResult(
      localOperator,
      localInput1,
      inputDataType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      localOutputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]]
      )
    val localOutput2 = getLocalResult(
      localOperator,
      localInput2,
      inputDataType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      localOutputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])

    val globalAgg = new BatchExecSortAggregate(
      cluster = FlinkRelOptClusterFactory.create(tableEnv.getPlanner, builder.getRexBuilder),
      relBuilder = tableEnv.getRelBuilder,
      traitSet = null,
      inputNode = null,
      aggCallToAggFunction = aggCallToAggFunction,
      rowRelDataType = outputRelDataType,
      inputRelDataType = localOutputRelDataType,
      grouping = Array(),
      auxGrouping = Array(),
      isMerge = true)
    val gloablOutRowType = FlinkTypeFactory.toInternalBaseRowTypeInfo(
      outputRelDataType, classOf[GenericRow])
    val ctx = CodeGeneratorContext(config, true)
    val generatedGlobalOperator = globalAgg.codegenWithoutKeys(
      isMerge = true,
      isFinal = true,
      ctx,
      tableEnv,
      DataTypes.internal(localOutputRowType).asInstanceOf[BaseRowType],
      DataTypes.internal(gloablOutRowType).asInstanceOf[BaseRowType],
      "Sort")
    val globalOperator = new OneInputSubstituteStreamOperator[BaseRow, BaseRow](
      generatedGlobalOperator.name,
      generatedGlobalOperator.code,
      references = ctx.references)

    // verify global agg result
    checkOperatorResult(
      globalOperator,
      localOutput1 ++ localOutput2,
      localOutputRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      Seq(expectedFinalResult),
      expectedFinalResultType.asInstanceOf[BaseRowTypeInfo[BaseRow]],
      gloablOutRowType.asInstanceOf[BaseRowTypeInfo[BaseRow]])
  }

  private def checkOperatorResult(
      operator: StreamOperator[BaseRow],
      inputData: Seq[BaseRow],
      inputType: BaseRowTypeInfo[BaseRow],
      expectedOutputData: Seq[BaseRow],
      expectedOutputType: BaseRowTypeInfo[BaseRow],
      actualOutputType: BaseRowTypeInfo[BaseRow]): Unit = {
    val result = getLocalResult(operator, inputData, inputType, actualOutputType)
    Assert.assertEquals("Output was not correct.", expectedOutputData.size, result.size)
    val config = new ExecutionConfig
    val actualFieldTypes = actualOutputType.getFieldTypes
    val expectedFieldTypes = expectedOutputType.getFieldTypes

    val equiv = result.zip(expectedOutputData).forall { case (actual, expert) =>
      if (actual.getArity == expert.getArity) {
        var ret = true
        for (index <- 0 until actual.getArity) {
          val expertField = if (expert.isNullAt(index)) {
            null
          } else {
            expert.get(
              index, expectedFieldTypes(index), expectedFieldTypes(index).createSerializer(config))
          }
          val actualField = if (actual.isNullAt(index)) {
            null
          } else {
            actual.get(
              index, actualFieldTypes(index), actualFieldTypes(index).createSerializer(config))
          }
          ret = ret && expertField == actualField
        }
        ret
      } else {
        false
      }
    }
    Assert.assertTrue("Output was not correct. " + result, equiv)
  }

  private def getLocalResult(
      operator: StreamOperator[BaseRow],
      inputData: Seq[BaseRow],
      inputType: TypeInformation[BaseRow],
      outputType: TypeInformation[BaseRow]): Seq[BaseRow] = {
    val taskFunc = new JFunction[Environment, OneInputStreamTask[BaseRow, BaseRow]] {
      override def apply(env: Environment): OneInputStreamTask[BaseRow, BaseRow] = {
        new OneInputStreamTask[BaseRow, BaseRow](env)
      }
    }
    val testHarness = new OneInputStreamTaskTestHarness[BaseRow, BaseRow](taskFunc,
      inputType, outputType)

    testHarness.setupOutputForSingletonOperatorChain()
    testHarness.getStreamConfig.setStreamOperator(operator)
    testHarness.getStreamConfig.setOperatorID(new OperatorID)

    testHarness.invoke()
    testHarness.waitForTaskRunning()
    for (row <- inputData) {
      testHarness.processElement(new StreamRecord[BaseRow](row, 0L))
    }
    testHarness.endInput()
    testHarness.waitForInputProcessing()
    testHarness.getOutput.toArray.map {
      a => a.asInstanceOf[StreamRecord[BaseRow]].getValue
    }
  }
}
