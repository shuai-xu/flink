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

package org.apache.flink.table.temptable

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.service.ServiceRegistryFactory
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{BaseRowType, DataType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink, TableSinkBase}
import org.apache.flink.table.temptable.FlinkTableServiceFactory.{TABLESERVICE_DEFAULT_READY_GAP_MS_VALUE, TABLESERVICE_DEFAULT_READY_RETRYTIMES_VALUE, TABLESERVICE_READY_RETRYGAP_MS, TABLESERVICE_READY_RETRYTIMES}
import org.apache.flink.table.temptable.rpc.TableServiceClient
import org.apache.flink.table.temptable.util.TableServiceUtil
import org.apache.flink.table.typeutils.BaseRowSerializer
import org.apache.flink.table.typeutils.TypeUtils
import org.apache.flink.table.util.TableProperties

/**
  * A built-in FlinkTableServiceSink.
  * This sink will write data to TableService.
  */
class FlinkTableServiceSink(
  clientClassName: String,
  tableProperties: TableProperties,
  tableName: String,
  resultType: BaseRowType) extends TableSinkBase[BaseRow]
  with BatchTableSink[BaseRow]
  with AppendStreamTableSink[BaseRow] {

  override protected def copy: TableSinkBase[BaseRow] =
    new FlinkTableServiceSink(clientClassName, tableProperties, tableName, resultType)

  override def emitBoundedStream(
    boundedStream: DataStream[BaseRow],
    tableConfig: TableConfig,
    executionConfig: ExecutionConfig): DataStreamSink[_] = {
    boundedStream.addSink(
      new FlinkTableServiceSinkFunction(clientClassName, tableProperties, tableName, resultType))
  }

  override def getOutputType: DataType = resultType

  /** Emits the DataStream. */
  override def emitDataStream(dataStream: DataStream[BaseRow]): DataStreamSink[_] = {
    dataStream.addSink(
      new FlinkTableServiceSinkFunction(
        clientClassName,
        tableProperties,
        tableName,
        resultType)
    )
  }


  override def getFieldNames: Array[String] = resultType.getFieldNames

  override def getFieldTypes: Array[DataType] =
    resultType.getFieldTypes.asInstanceOf[Array[DataType]]
}

/**
  * Built-in SinkFunction for FlinkTableServiceSink.
  */
class FlinkTableServiceSinkFunction(
  clientClassName: String,
  tableProperties: TableProperties,
  tableName: String,
  resultType: BaseRowType)
  extends RichSinkFunction[BaseRow] {

  private var partitionId: Int = _

  private var flinkTableServiceClient: TableServiceClient = _

  private var baseRowSerializer: BaseRowSerializer[_ <: BaseRow] = _

  override def open(parameters: Configuration): Unit = {
    partitionId = getRuntimeContext.getIndexOfThisSubtask
    flinkTableServiceClient = Class.forName(clientClassName)
      .newInstance().asInstanceOf[TableServiceClient]
    flinkTableServiceClient.setRegistry(ServiceRegistryFactory.getRegistry)
    flinkTableServiceClient.open(tableProperties)
    baseRowSerializer =
      TypeUtils.createSerializer(resultType).asInstanceOf[BaseRowSerializer[BaseRow]]
    val maxRetry = parameters
      .getInteger(TABLESERVICE_READY_RETRYTIMES, TABLESERVICE_DEFAULT_READY_RETRYTIMES_VALUE)
    val retryGap = parameters
      .getLong(TABLESERVICE_READY_RETRYGAP_MS, TABLESERVICE_DEFAULT_READY_GAP_MS_VALUE)
    TableServiceUtil.checkTableServiceReady(flinkTableServiceClient, maxRetry, retryGap)
    // send initialize Partition request to delete existing data.
    flinkTableServiceClient.initializePartition(tableName, partitionId)
  }

  override def close(): Unit = {
    if (flinkTableServiceClient != null) {
      flinkTableServiceClient.close()
    }
  }

  override def invoke(value: BaseRow, context: SinkFunction.Context[_]): Unit = {
    flinkTableServiceClient.write(tableName, partitionId, value, baseRowSerializer)
  }
}
