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

package org.apache.flink.table.validate

import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.catalog.CatalogFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedAggFunctions.Accumulator0
import org.junit.Test

/**
  * Test for FunctionCatalogUtils.
  */
class FunctionCatalogUtilsTest {

  private val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
  private val funcName = "test"

  @Test
  def testInitiateScalarFunction(): Unit = {
    val catalogFunc = new CatalogFunction(classOf[TestScalarFunction].getName)

    val sqlFunc = FunctionCatalogUtils.toSqlFunction(funcName, catalogFunc, typeFactory)

    assert(sqlFunc != null)
  }

  @Test
  def testInitiateTableFunction(): Unit = {
    val catalogFunc = new CatalogFunction(classOf[TestTableFunction].getName)

    val sqlFunc = FunctionCatalogUtils.toSqlFunction(funcName, catalogFunc, typeFactory)

    assert(sqlFunc != null)
  }

  @Test
  def testInitiateAggregateFunction(): Unit = {
    val catalogFunc = new CatalogFunction(classOf[TestAggregateFunction].getName)

    val sqlFunc = FunctionCatalogUtils.toSqlFunction(funcName, catalogFunc, typeFactory)

    assert(sqlFunc != null)
  }

  @Test (expected = classOf[RuntimeException])
  def testInitiateSingletonFunction(): Unit = {
    val catalogFunc = new CatalogFunction(TestSingletonFunction.getClass.getName)

    val sqlFunc = FunctionCatalogUtils.toSqlFunction(funcName, catalogFunc, typeFactory)
  }

}

class TestScalarFunction extends ScalarFunction {
  def eval(s: String): Int = s.hashCode()
}

object TestSingletonFunction extends ScalarFunction {
  def eval(s: String): Int = s.hashCode()
}

class TestTableFunction extends TableFunction[String] {
  def eval(str: String): Unit = {
    if (str.contains("#")){
      str.split("#").foreach(collect)
    }
  }
}

class TestAggregateFunction extends AggregateFunction[Long, Accumulator0] {
  override def createAccumulator = new Accumulator0

  override def getValue(accumulator: Accumulator0) = 1L

  //Overloaded accumulate method
  def accumulate(accumulator: Accumulator0, iValue: Long, iWeight: Int): Unit = {
  }

  override def requiresOver = true
}

