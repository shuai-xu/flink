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

package org.apache.flink.table.runtime.stream.table

import java.io.File
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamTestData, StreamingTestBase}
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.types.{DataType, DataTypes}
import org.apache.flink.table.util.MemoryTableSourceSinkUtil
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test

import scala.collection.JavaConverters._

class TableSinkITCase extends StreamingTestBase {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @Test
  def testInsertIntoRegisteredTableSink(): Unit = {
    MemoryTableSourceSinkUtil.clear

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(r => r._2)
    val fieldNames = Array("d", "e", "t")
    val fieldTypes: Array[DataType] = Array(DataTypes.STRING, DataTypes.TIMESTAMP, DataTypes.LONG)
    val sink = new MemoryTableSourceSinkUtil.UnsafeMemoryAppendTableSink
    tEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)

    input.toTable(tEnv, 'a, 'b, 'c, 't.rowtime)
      .where('a < 3 || 'a > 19)
      .select('c, 't, 'b)
      .insertInto("targetTable")
    env.execute()

    val expected = Seq(
      "Hi,1970-01-01 00:00:00.001,1",
      "Hello,1970-01-01 00:00:00.002,2",
      "Comment#14,1970-01-01 00:00:00.006,6",
      "Comment#15,1970-01-01 00:00:00.006,6").mkString("\n")

    TestBaseUtils.compareResultAsText(MemoryTableSourceSinkUtil.results.asJava, expected)
  }

  @Test
  def testStreamTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val input = StreamTestData.get3TupleDataStream(env)
      .assignAscendingTimestamps(_._2)
      .map(x => x).setParallelism(4) // increase DOP to 4

    input.toTable(tEnv, 'a, 'b.rowtime, 'c)
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .writeToSink(
        new CsvTableSink(path, Some(","), None, None, None, Some(WriteMode.OVERWRITE), None, None))

    env.execute()

    val expected = Seq(
      "Hi,1970-01-01 00:00:00.001",
      "Hello,1970-01-01 00:00:00.002",
      "Hello world,1970-01-01 00:00:00.002",
      "Hello world, how are you?,1970-01-01 00:00:00.003",
      "Comment#12,1970-01-01 00:00:00.006",
      "Comment#13,1970-01-01 00:00:00.006",
      "Comment#14,1970-01-01 00:00:00.006",
      "Comment#15,1970-01-01 00:00:00.006").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }
}
