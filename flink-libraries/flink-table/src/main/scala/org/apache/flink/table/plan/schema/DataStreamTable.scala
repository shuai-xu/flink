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

package org.apache.flink.table.plan.schema

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.plan.stats.{FlinkStatistic, TableStats}
import org.apache.flink.table.types.DataTypes

class DataStreamTable[T](
    val dataStream: DataStream[T],
    val producesUpdates: Boolean,
    val isAccRetract: Boolean,
    tableSchema: TableSchema,
    statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends FlinkTable(DataTypes.of(dataStream.getType), tableSchema, statistic) {

  def this(source: DataStream[T], producesUpdates: Boolean, isAccRetract: Boolean) {
    this(source, producesUpdates, isAccRetract, TableSchema.fromTypeInfo(source.getType))
  }

  // This is only used for boundedStream now, we supply default statistic.
  def this(boundedStream: DataStream[T]) {
    this(boundedStream, false, false, TableSchema.fromTypeInfo(boundedStream.getType),
      FlinkStatistic.of(TableStats(1000L)))
  }

  // This is only used for boundedStream now, we supply default statistic.
  def this(boundedStream: DataStream[T], tableSchema: TableSchema) {
    this(boundedStream, false, false, tableSchema, FlinkStatistic.of(TableStats(1000L)))
  }

  def this(source: DataStream[T], tableSchema: TableSchema, statistic: FlinkStatistic) {
    this(source, false, false, tableSchema, statistic)
  }

  override def copy(statistic: FlinkStatistic): FlinkTable = {
    new DataStreamTable[T](dataStream, producesUpdates, isAccRetract, tableSchema, statistic)
  }
}
