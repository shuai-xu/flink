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

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.factories.utils.TestingTableSink
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.util.TableProperties
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableRegisterITCase extends StreamingTestBase {

  @Test
  def test(): Unit = {
    TestingTableSink.globalResults.clear()

    val tableSource = "tableSource"
    tEnv.registerTable(tableSource)
      .withSchema(
        new TableSchema.Builder()
          .column("a", DataTypes.INT)
          .column("b", DataTypes.LONG)
          .column("c", DataTypes.STRING).build())
      .withProperties(
        new TableProperties()
          .property("connector.type", "test")
      )

    val tableSink = "tableSink"
    tEnv.registerTable(tableSink)
      .withSchema(
        new TableSchema.Builder()
          .column("a", DataTypes.INT)
          .column("b", DataTypes.LONG)
          .column("c", DataTypes.STRING).build())
      .withProperties(
        new TableProperties()
          .property("connector.type", "test")
      )

    tEnv.scan(tableSource)
      .where('a >= 2)
      .insertInto(tableSink)

    tEnv.execute()

    val expected = mutable.MutableList(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, TestingTableSink.globalResults.sorted)
  }
}
