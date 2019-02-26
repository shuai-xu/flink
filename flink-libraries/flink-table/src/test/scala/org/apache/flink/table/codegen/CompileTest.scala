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

package org.apache.flink.table.codegen

import org.apache.flink.api.common.InvalidProgramException

import org.junit.{Assert, Test}

import java.net.{URL, URLClassLoader}

class CompileTest {

  @Test
  def testCacheReuse(): Unit = {
    val code =
      s"""
         |public class Main {
         |  int i;
         |  int j;
         |}
       """.stripMargin

    val clz = CodeGenUtils.compile(this.getClass.getClassLoader, "Main", code)
    Assert.assertTrue(clz == CodeGenUtils.compile(this.getClass.getClassLoader, "Main", code))
    Assert.assertFalse(clz == CodeGenUtils.compile(new TestClassLoader, "Main", code))
  }

  @Test
  def testWrongCode(): Unit = {
    val main =
      s"""
         |public class111 Main {
         |  int i;
         |  int j;
         |}
       """.stripMargin

    try {
      CodeGenUtils.compile(this.getClass.getClassLoader, "Main", main)
    } catch {
      case e: InvalidProgramException =>
    }
  }
}

private class TestClassLoader extends URLClassLoader(
  new Array[URL](0), Thread.currentThread.getContextClassLoader) {
}
