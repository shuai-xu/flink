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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.runtime.batch.sql.QueryTest.row
import org.apache.flink.table.runtime.batch.sql.TestData.{nullData3, nullablesOfNullData3}
import org.apache.flink.types.Row
import org.junit._

class ComplexTypeITCase extends QueryTest {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 3)
    registerCollection(
      "T",
      nullData3.map((r) => row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes)),
      new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3)
  }

  @Test
  def testCalcBinary(): Unit = {
    checkResult(
      "select a, b, c from T where b < 1000",
      nullData3.map((r) => row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes))
    )
  }

  @Test
  def testOrderByBinary(): Unit = {
    conf.getParameters.setInteger(TableConfig.SQL_EXEC_DEFAULT_PARALLELISM, 1)
    conf.getParameters.setBoolean(TableConfig.SQL_EXEC_SORT_ENABLE_RANGE, true)
    checkResult(
      "select * from T order by c",
      nullData3.sortBy((x : Row) =>
        x.getField(2).asInstanceOf[String]).map((r) =>
        row(r.getField(0), r.getField(1), r.getField(2).toString.getBytes)),
      isSorted = true
    )
  }

  @Test
  def testGroupByBinary(): Unit = {
    registerCollection(
      "T2",
      nullData3.map((r) => row(r.getField(0), r.getField(1).toString.getBytes, r.getField(2))),
      new RowTypeInfo(INT_TYPE_INFO, BYTE_PRIMITIVE_ARRAY_TYPE_INFO, STRING_TYPE_INFO),
      "a, b, c",
      nullablesOfNullData3)
    checkResult(
      "select sum(sumA) from (select sum(a) as sumA, b, c from T2 group by c, b) group by b",
      Seq(row(1), row(111), row(15), row(34), row(5), row(65), row(null))
    )
  }
}
