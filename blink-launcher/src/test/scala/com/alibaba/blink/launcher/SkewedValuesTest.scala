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
package com.alibaba.blink.launcher

import java.util.Properties

import com.alibaba.blink.launcher.util.SqlJobAdapter
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.CommonTestData.{get3Data, writeToTempFile}
import org.apache.flink.table.util.ExecResourceUtil.InferMode
import org.junit.Test

class SkewedValuesTest {

  @Test
  def testSkewedValues(): Unit = {

    val fieldDelimiter = ","
    val lineDelimiter = CsvInputFormat.DEFAULT_LINE_DELIMITER
    val csvRecords = get3Data().map{
      case (f1, f2, f3) => s"$f1$fieldDelimiter$f2$fieldDelimiter$f3"
    }
    val file = writeToTempFile(csvRecords.mkString(lineDelimiter), "SkewedValuesTest", "tmp")

    val sql  =
      s"""
         |CREATE TABLE test_source (
         |  a int,
         |  b bigint,
         |  c varchar
         |) with (
         |  type = 'CSV',
         |  path = '$file',
         |  fieldDelim = ',',
         |  skew_values= '{a: [1,2]}'
         |);
      """.stripMargin

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    tEnv.getConfig.getParameters.setString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE,
      InferMode.ONLY_SOURCE.toString)

    val sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql)
    SqlJobAdapter.registerTables(
      tEnv,
      sqlNodeInfoList,
      new Properties(),
      Thread.currentThread().getContextClassLoader)

    val t = tEnv.scan("test_source")
    val results = t.join(t.select('a as 'd, 'b as 'e, 'c as 'f)).collect()
    assert(results.length == 441)
  }
}
