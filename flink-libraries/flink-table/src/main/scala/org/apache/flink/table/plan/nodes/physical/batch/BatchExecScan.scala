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

package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple}
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfig, TableEnvironment}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.plan.nodes.common.CommonScan
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.util.Logging

import java.lang.{Boolean => JBoolean}
import java.lang.{Integer => JInteger}

trait BatchExecScan extends CommonScan[BinaryRow] with RowBatchExecRel with Logging {

  private[flink] var sourceResSpec: ResourceSpec = _
  private[flink] var conversionResSpec: ResourceSpec = _

  // This rel needs two resourceSpec, so we should set detail transformations res here.
  // TODO split BatchExecScan to every rel only do a work.
  def setResForSourceAndConversion(
      sourceResSpec: ResourceSpec,
      conversionResSpec: ResourceSpec): Unit = {
    this.sourceResSpec = sourceResSpec
    this.conversionResSpec = conversionResSpec
  }

  // get resultPartitionNum set on source transformation. The returned type (left, right)
  // represents isParallelismLocked and the current set parallelism.
  private[flink] def getTableSourceResultPartitionNum(
      tableEnv: TableEnvironment): JTuple[JBoolean, JInteger]

  // get resourceSpec set on source transformation.
  private[flink] def getTableSourceResource(tableEnv: TableEnvironment): ResourceSpec

   /**
    * Assign source for transformation.
    *
    */
  def assignSourceResourceAndParallelism(
      tableEnv: BatchTableEnvironment,
      input: StreamTransformation[Any]): Unit = {

    input.setParallelism(resultPartitionCount)
    input.setResources(sourceResSpec, sourceResSpec)
    tableEnv.getRUKeeper().addTransformation(this, input)
  }

  def convertToInternalRow(
      tableEnv: BatchTableEnvironment,
      input: StreamTransformation[Any],
      fieldIdxs: Array[Int],
      outRowType: RelDataType,
      dataType: DataType,
      config: TableConfig,
      rowtimeExpr: Option[RexNode]): StreamTransformation[BaseRow] = {
    if (needInternalConversion) {
      val ctx = CodeGeneratorContext(config, supportReference = true)
      val convertTransform = convertToInternalRow(
        ctx, input, fieldIdxs, dataType, outRowType, getTable.getQualifiedName, config, rowtimeExpr)
      convertTransform.setParallelismLocked(true)
      convertTransform.setResources(conversionResSpec, conversionResSpec)
      tableEnv.getRUKeeper().addTransformation(this, convertTransform)
      convertTransform
    } else {
      input.asInstanceOf[StreamTransformation[BaseRow]]
    }
  }
}
