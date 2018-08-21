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

import com.alibaba.cinder.impl.JavaSourceRewriter

object JavaSourceManipulator {

  private var MAX_METHOD_LENGTH_FOR_JVM: Long = 48000L

  private var MAX_METHOD_LENGTH_FOR_JIT: Long = 8000L

  private var MAX_FIELD_COUNT: Int = 10000

  private var REWRITE_ENABLE: Boolean = false

  def getMaxMethodLengthForJvm: Long = MAX_METHOD_LENGTH_FOR_JVM

  def setMaxMethodLengthForJvm(maxMethodLengthForJvm: Long): Unit = {
    MAX_METHOD_LENGTH_FOR_JVM = maxMethodLengthForJvm
  }

  def getMaxMethodLengthForJit: Long = MAX_METHOD_LENGTH_FOR_JIT

  def setMaxMethodLengthForJit(maxMethodLengthForJit: Long): Unit = {
    MAX_METHOD_LENGTH_FOR_JIT = maxMethodLengthForJit
  }

  def getMaxFieldCount: Int = MAX_FIELD_COUNT

  def setMaxFieldCount(maxFieldCount: Int) = {
    MAX_FIELD_COUNT = maxFieldCount
  }

  def isRewriteEnable: Boolean = REWRITE_ENABLE

  def setRewriteEnable(rewriteEnable: Boolean): Unit = {
    REWRITE_ENABLE = rewriteEnable
  }

  def rewrite(code: String): String = {
    if (REWRITE_ENABLE) {
      val rewriter = new JavaSourceRewriter(
        code,
        MAX_METHOD_LENGTH_FOR_JVM,
        MAX_METHOD_LENGTH_FOR_JIT,
        MAX_FIELD_COUNT
      )
      rewriter.apply()
    } else {
      code
    }
  }
}
