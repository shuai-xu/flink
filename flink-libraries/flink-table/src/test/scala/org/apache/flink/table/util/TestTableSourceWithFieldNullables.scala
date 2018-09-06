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

package org.apache.flink.table.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.sources.{BatchExecTableSource, DefinedFieldNullables}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.types.Row

class TestTableSourceWithFieldNullables(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    fieldNullables: Array[Boolean])
    extends BatchExecTableSource[Row]
        with DefinedFieldNullables {

  if (fieldNames.length != fieldTypes.length) {
    throw TableException("Number of field names and field types must be equal.")
  }

  if (fieldNames.length != fieldNullables.length) {
    throw TableException("Number of field names and field nullables must be equal.")
  }

  override def getReturnType: DataType =
    DataTypes.of(new RowTypeInfo(fieldTypes, fieldNames))

  override def getTableStats: TableStats = null

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[Row] = null

  /** Returns the nullable properties of the table fields. */
  override def getFieldNullables: Array[Boolean] = fieldNullables
}
