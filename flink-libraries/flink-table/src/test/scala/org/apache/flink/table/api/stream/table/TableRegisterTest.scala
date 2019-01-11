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

package org.apache.flink.table.api.stream.table

import org.apache.flink.table.api.{TableInfo, TableSchema}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.util.{TableProperties, TableTestBase}
import org.junit.Test

class TableRegisterTest extends TableTestBase {

  @Test
  def testTableSchema(): Unit = {
    val util = streamTestUtil()

    val tableSource = "tableSource"
    TableInfo.create(util.tableEnv)
      .withSchema(
        new TableSchema.Builder()
          .column("a", DataTypes.INT)
          .column("b", DataTypes.LONG)
          .column("c", DataTypes.STRING)
          .computedColumn("d", "a.toTimestamp")
          .watermark("wk1", "d", 0)
          .primaryKey("c").build())
      .withProperties(
        new TableProperties()
          .property("connector.type", "test"))
      .registerTableSource(tableSource)

    val resultTable = util.tableEnv.scan(tableSource)
      .where('a > 9)

    util.verifyPlan(resultTable)
  }
}
