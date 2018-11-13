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

package com.alibaba.blink.launcher.pyflink


import java.io.File
import java.nio.file.Files

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.runtime.utils._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.{After, Assert, Ignore, Test}
import java.util.Properties

import com.alibaba.blink.launcher.{JobLauncher, TestUtil}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.runtime.functions.python.PythonScalarFunction

class PythonUdfTest {

  @Test
  @Ignore
  def testUdf(): Unit = {
    val py = "pyflink/test_basic_types.py"
    val userPyLibs = TestUtil.getResourcePath(py)

    val conf = new TableConfig
    val jobConf = new Properties

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, conf)

    // register python files as libraries.
    try
      JobLauncher.registerPythonLibFiles(env.getJavaEnv, jobConf, userPyLibs)
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        Assert.fail("Can't register python lib files. " + ex.getMessage)
    }

    // register functions
    val udfs = Map("upper_udf" -> ("test_basic_types.upper_udf", DataTypes.STRING),
      "integer_udf" -> ("test_basic_types.integer_udf", DataTypes.INT)
    )
    udfs.foreach(u => {
      val scalarFunction: ScalarFunction = new PythonScalarFunction(u._1, u._2._1, u._2._2)
      tEnv.registerFunction(u._1, scalarFunction)
    })

    val data = List(
      ("Pink Floyd", 53),
      ("Dead Can Dance", 37)
    )

    // set config
    val propMap = jobConf.asInstanceOf[java.util.Map[String, String]]
    env.getConfig.setGlobalJobParameters(ParameterTool.fromMap(propMap))

    // register table
    val t1 = env.fromCollection(data).toTable(tEnv).as('band, 'years)
    tEnv.registerTable("T1", t1)

    // integer_udf = return x + 1;
    val sqlQuery =
      """
        | SELECT upper_udf(band), integer_udf(years)
        | FROM T1
        | WHERE integer_udf(years) < 70 AND integer_udf(years) > 20
      """.stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)

    env.execute()

    val expected = List(
      "PINK FLOYD,54",
      "DEAD CAN DANCE,38"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @After
  def clean(): Unit = {
    val workingDir = java.nio.file.Paths.get(".").toAbsolutePath.normalize.toString
    // now, distributed cache won't clean the local soft symbolic links
    // here, delete them manually
    val files = Array("test_basic_types.py", "test_basic_types.pyc")
    for (f <- files) {
      try {
        val file = new File(workingDir + File.separator + f)
        Files.deleteIfExists(file.toPath)
      } catch {
        case ex: Exception => ex.printStackTrace()
      }
    }
  }

}
