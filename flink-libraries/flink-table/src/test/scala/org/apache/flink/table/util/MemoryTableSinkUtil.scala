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

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.operators.StreamSink
import org.apache.flink.streaming.api.transformations.SinkTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchExecTableSink, TableSinkBase}
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.types.Row

import scala.collection.mutable

object MemoryTableSinkUtil {
  var results: mutable.MutableList[String] = mutable.MutableList.empty[String]

  def clear = {
    MemoryTableSinkUtil.results.clear()
  }

  final class UnsafeMemoryAppendTableSink
    extends TableSinkBase[Row]
    with BatchExecTableSink[Row]
    with AppendStreamTableSink[Row]
    with DefinedDistribution {

    override def getOutputType: DataType = {
      DataTypes.createRowType(getFieldTypes, getFieldNames)
    }

    override protected def copy: TableSinkBase[Row] = {
      val sink = new UnsafeMemoryAppendTableSink
      sink.pk = this.pk
      sink
    }

    override def emitDataStream(dataStream: DataStream[Row]): Unit = {
      dataStream.addSink(new MemoryAppendSink)
    }

    private var pk: String = null

    def setPartitionedField(pk: String): Unit = {
      this.pk = pk
    }

    override def getPartitionField(): String = pk

    override def shuffleEmptyKey(): Boolean = false

    /** Emits the BoundedStream. */
    override def emitBoundedStream(
      boundedStream: DataStream[Row],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[Row] = {
      val ret = boundedStream.addSink(new MemoryAppendSink).name("MemorySink")
      ret.setParallelism(1)
      ret.getTransformation.setParallelismLocked(true)
      ret
    }
  }

  private class MemoryAppendSink extends RichSinkFunction[Row]() {

    override def invoke(value: Row): Unit = {
      results.synchronized {
        results += value.toString
      }
    }
  }

  private class MemoryCollectionOutputFormat extends RichOutputFormat[Row] {

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {}

    override def writeRecord(record: Row): Unit = {
      results.synchronized {
        results += record.toString
      }
    }

    override def close(): Unit = {}
  }
}
