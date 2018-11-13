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

package org.apache.flink.table.plan.nodes.common

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, InputFormatCodeGenerator}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.runtime.io.ValuesInputFormat
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import scala.collection.JavaConversions._

abstract class CommonValue(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowRelDataType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
  description: String)
  extends Values(cluster, rowRelDataType, tuples, traitSet) {

  override def deriveRowType(): RelDataType = rowRelDataType

  protected def generatorInputFormat(
    tableEnv: TableEnvironment): ValuesInputFormat = {
    val config = tableEnv.getConfig

    val outputType = FlinkTypeFactory.toInternalBaseRowType(getRowType, classOf[GenericRow])

    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, false, config.getNullCheck)

    // generate code for every record
    val generatedRecords = getTuples.map { r =>
      exprGenerator.generateResultExpression(r.map(exprGenerator.generateExpression), outputType)
    }

    // generate input format
    val generatedFunction = InputFormatCodeGenerator.generateValuesInputFormat(
      ctx,
      description,
      generatedRecords.map(_.code),
      outputType)

    new ValuesInputFormat(
      generatedFunction.name,
      generatedFunction.code,
      DataTypes.toTypeInfo(outputType).asInstanceOf[BaseRowTypeInfo[BaseRow]]
    )
  }
}
