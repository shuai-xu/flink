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

import org.junit.Test

/**
  * Test LowerCaseKeyMap functions.
  */
class LowerCaseKeyMapTest {

  @Test
  def testGetWithUpperLowerCase(): Unit = {
    val m = Map(
      "aaa" -> 1,
      "aAa" -> 2,
      "aA中国人" -> 3,
      "\n\t*,.`" -> 4
    )
    val lm = LowerCaseKeyMap(m)
    assert(lm.get("aaa").get == 2, "Key aaa expect to be overridden.")
    assert(lm.get("aa中国人").get == 3)
    assert(lm.get("AA中国人").get == 3)
    assert(lm.get("\n\t*,.`").get == 4)
  }

  @Test
  def testPlusAndMinus(): Unit = {
    val m = Map(
      "aaa" -> 1,
      "aAa" -> 2,
      "aA中国人" -> 3,
      "\n\t*,.`" -> 4
    )
    var lm: Map[String, Any] = LowerCaseKeyMap(m)
    lm += ("bBb" -> 5)
    assert(lm("bbb") == 5)
    lm -= "aaa"
    assert(lm.get("aaa").isEmpty)
    // the original map should not be changed.
    val m1 = Map(
      "aaa" -> 1,
      "aAa" -> 2,
      "aA中国人" -> 3,
      "\n\t*,.`" -> 4
    )
    assert(m1 == m, "LowerCaseKeyMap should not change the underlying map.")
  }

  @Test
  def testIterator(): Unit = {
    val m = Map(
      "aaa" -> 1,
      "aAa" -> 2,
      "aA中国人" -> 3,
      "\n\t*,.`" -> 4
    )
    val lm: Map[String, Any] = LowerCaseKeyMap(m)
    assert(lm.keySet == Set("aaa", "aa中国人", "\n\t*,.`"))
  }
}
