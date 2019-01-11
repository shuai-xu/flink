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

package org.apache.flink.connectors.hbase.table

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.connectors.hbase.HTableSchema
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import java.lang.{Boolean => JBool}
import java.util.{Map => JMap}
import java.util.{List => JList}
import org.apache.flink.connectors.hbase.streaming.HBase143Writer

class HBase143UpsertTableSink(
    sqlSchema: RichTableSchema,
    hbaseSchema: HTableSchema,
    rowkeyIndex: Integer,
    qualifierIndexes: JList[Integer],
    configuration: HConfiguration,
    userParamMap: JMap[String, String])
  extends TableSinkBase[JTuple2[JBool, Row]] with UpsertStreamTableSink[Row] with
    BatchCompatibleStreamTableSink[JTuple2[JBool, Row]] {

  //"given sql and hbase schemas should not be null!"
  Preconditions.checkArgument(null != sqlSchema && null != hbaseSchema)

  // only support single sql primary key mapping to hbase rowKey for now.
  Preconditions.checkArgument(1 == sqlSchema.getPrimaryKeys.size)

  val hbaseQualifierCnt: Int = hbaseSchema.getFlatQualifiers.size
  val sqlColumnCnt: Int = sqlSchema.getColumnNames.length

  // the given hbase schema's qualifier number should consist with sql schema's column number
  Preconditions.checkArgument(hbaseQualifierCnt == sqlColumnCnt)

  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    new HBase143UpsertTableSink(
      sqlSchema,
      hbaseSchema,
      rowkeyIndex,
      qualifierIndexes,
      configuration,
      userParamMap)
  }

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    val sink = new HBase143Writer(hbaseSchema, rowkeyIndex, qualifierIndexes);
    dataStream.addSink(sink).name(sink.toString)
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]])
  : DataStreamSink[_] = {
    val sink = new HBase143Writer(hbaseSchema, rowkeyIndex, qualifierIndexes);
    boundedStream.addSink(sink).name(sink.toString)
  }
}
