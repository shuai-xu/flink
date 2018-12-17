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

package org.apache.flink.table.runtime

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{BaseRowType, DataTypes}
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.util.BinaryRowUtil
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow}
import org.apache.flink.table.typeutils.TypeCheckUtils.validateEqualsHashCode
import org.apache.flink.table.typeutils.{BaseRowSerializer, BaseRowTypeInfo}

abstract class BaseRowKeySelector
  extends KeySelector[BaseRow, BaseRow] with ResultTypeQueryable[BaseRow]

class BinaryRowKeySelector(
  keyFields: Array[Int],
  inputType: BaseRowTypeInfo[_])
  extends BaseRowKeySelector {

  @transient lazy private val returnType: BaseRowTypeInfo[BaseRow] =
    new BaseRowTypeInfo(classOf[BinaryRow], keyFields.map(inputType.getFieldTypes()(_)): _*)
      .asInstanceOf[BaseRowTypeInfo[BaseRow]]

  @transient lazy private val gProjection: GeneratedProjection = ProjectionCodeGenerator
    .generateProjection(CodeGeneratorContext.apply(new TableConfig, supportReference = false),
      classOf[BaseRowSerializer[_ <: BaseRow]].getSimpleName,
      DataTypes.internal(inputType).asInstanceOf[BaseRowType],
      DataTypes.internal(returnType).asInstanceOf[BaseRowType],
      keyFields)

  @transient lazy private val projection: Projection[BaseRow, BinaryRow] = {
    val ret = CodeGenUtils.compile(
      Thread.currentThread.getContextClassLoader, gProjection.name, gProjection.code)
      .newInstance.asInstanceOf[Projection[BaseRow, BinaryRow]]
    gProjection.code = null
    ret
  }

  // check if type implements proper equals/hashCode
  validateEqualsHashCode("grouping", returnType)

  override def getKey(value: BaseRow): BaseRow = projection(value).copy()

  override def getProducedType: BaseRowTypeInfo[BaseRow] = returnType
}

class NullBinaryRowKeySelector extends BaseRowKeySelector {
  @transient lazy val returnType: BaseRowTypeInfo[BaseRow] = new BaseRowTypeInfo(
    classOf[BinaryRow]).asInstanceOf[BaseRowTypeInfo[BaseRow]]

  @transient lazy val row: BinaryRow = BinaryRowUtil.EMPTY_ROW

  // check if type implements proper equals/hashCode
  validateEqualsHashCode("grouping", returnType)

  override def getKey(value: BaseRow): BaseRow = row
  override def getProducedType: BaseRowTypeInfo[BaseRow] = returnType
}


