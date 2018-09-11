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
package org.apache.flink.table.runtime.utils

import java.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.CoreOptions
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.FromElementsFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.{HEAP_BACKEND, NIAGARA_BACKEND, StateBackendMode}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.rules.TemporaryFolder
import org.junit.runners.Parameterized
import org.junit.{After, Assert, Before, Rule}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class StreamingWithStateTestBase(state: StateBackendMode)
  extends AbstractTestBase {

  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  def before(): Unit = {
    StreamTestSink.clear()
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // set state backend
//    state match {
//      case HEAP_BACKEND =>
//        env.setConfiguration(CoreOptions.STATE_BACKEND_CLASSNAME,
//          classOf[HeapInternalStateBackend].getCanonicalName)
//
//      case NIAGARA_BACKEND =>
//        env.getConfiguration.setString(
//          CoreOptions.STATE_BACKEND_CLASSNAME,
//          classOf[NiagaraStateBackend].getCanonicalName)
//    }
    this.tEnv = TableEnvironment.getTableEnvironment(env)
  }

  @After
  def after(): Unit = {
    Assert.assertTrue(FailingCollectionSource.failedBefore)
  }

  /**
    * Creates a DataStream from the given non-empty [[Seq]].
    */
  def failingDataSource[T: TypeInformation](data: Seq[T]): DataStream[T] = {
    state match {
      case NIAGARA_BACKEND =>
        // TODO: introducing state recovery may cause NIAGARA core dump, see BLINK-15412517
        // TODO: please revert this after BLINK-15412517 is fixed
        FailingCollectionSource.failedBefore = true
        env.fromCollection(data)
      case _ =>
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0))
        // reset failedBefore flag to false
        FailingCollectionSource.reset()

        require(data != null, "Data must not be null.")
        val typeInfo = implicitly[TypeInformation[T]]

        val collection = scala.collection.JavaConversions.asJavaCollection(data)
        // must not have null elements and mixed elements
        FromElementsFunction.checkCollection(data, typeInfo.getTypeClass)

        val function = new FailingCollectionSource[T](
          typeInfo.createSerializer(env.getConfig),
          collection,
          data.length / 2) // fail after half elements

        env.addSource(function)(typeInfo).setMaxParallelism(1)
    }
  }

  private def mapStrEquals(str1: String, str2: String): Boolean = {
    val array1 = str1.toCharArray
    val array2 = str2.toCharArray
    if (array1.length != array2.length) {
      return false
    }
    val l = array1.length
    val leftBrace = "{".charAt(0)
    val rightBrace = "}".charAt(0)
    val equalsChar = "=".charAt(0)
    val lParenthesis = "(".charAt(0)
    val rParenthesis = ")".charAt(0)
    val dot = ",".charAt(0)
    val whiteSpace = " ".charAt(0)
    val map1 = Map[String, String]()
    val map2 = Map[String, String]()
    var idx = 0
    def findEquals(ss: CharSequence): Array[Int] = {
      val ret = new ArrayBuffer[Int]()
      (0 until ss.length) foreach (idx => if (ss.charAt(idx) == equalsChar) ret += idx)
      ret.toArray
    }

    def splitKV(ss: CharSequence, equalsIdx: Int): (String, String) = {
      // find right, if starts with '(' find until the ')', else until the ','
      var endFlag = false
      var curIdx = equalsIdx + 1
      var endChar = if (ss.charAt(curIdx) == lParenthesis) rParenthesis else dot
      var valueStr: CharSequence = null
      var keyStr: CharSequence = null
      while (curIdx < ss.length && !endFlag) {
        val curChar = ss.charAt(curIdx)
        if (curChar != endChar && curChar != rightBrace) {
          curIdx += 1
          if (curIdx == ss.length) {
            valueStr = ss.subSequence(equalsIdx + 1, curIdx)
          }
        } else {
          valueStr = ss.subSequence(equalsIdx + 1, curIdx)
          endFlag = true
        }
      }

      // find left, if starts with ')' find until the '(', else until the ' ,'
      endFlag = false
      curIdx = equalsIdx - 1
      endChar = if (ss.charAt(curIdx) == rParenthesis) lParenthesis else whiteSpace
      while (curIdx >= 0 && !endFlag) {
        val curChar = ss.charAt(curIdx)
        if (curChar != endChar && curChar != leftBrace) {
          curIdx -= 1
          if (curIdx == -1) {
            keyStr = ss.subSequence(0, equalsIdx)
          }
        } else {
          keyStr = ss.subSequence(curIdx, equalsIdx)
          endFlag = true
        }
      }
      require(keyStr != null)
      require(valueStr != null)
      (keyStr.toString, valueStr.toString)
    }

    def appendStrToMap(ss: CharSequence, m: Map[String, String]): Unit = {
      val equalsIdxs = findEquals(ss)
      equalsIdxs.foreach (idx => m + splitKV(ss, idx))
    }

    while (idx < l) {
      val char1 = array1(idx)
      val char2 = array2(idx)
      if (char1 != char2) {
        return false
      }

      if (char1 == leftBrace) {
        val rightBraceIdx = array1.subSequence(idx+1, l).toString.indexOf(rightBrace)
        appendStrToMap(array1.subSequence(idx+1, rightBraceIdx + idx + 2), map1)
        idx += rightBraceIdx
      } else {
        idx += 1
      }
    }
    map1.equals(map2)
  }

  def assertMapStrEquals(str1: String, str2: String): Unit = {
    if (!mapStrEquals(str1, str2)) {
      throw new AssertionError(s"Expected: $str1 \n Actual: $str2")
    }
  }
}

object StreamingWithStateTestBase {

  case class StateBackendMode(backend: String) {
    override def toString: String = backend.toString
  }

  val HEAP_BACKEND = StateBackendMode("HEAP")
  val NIAGARA_BACKEND = StateBackendMode("NIAGARA")

  @Parameterized.Parameters(name = "StateBackend={0}")
  def parameters(): util.Collection[Array[java.lang.Object]] = {
    val isLinuxAliOS = System.getProperty("os.name").startsWith("Linux") &&
      System.getProperty("os.version").contains("alios7")

    if (isLinuxAliOS) {
      Seq[Array[AnyRef]](Array(HEAP_BACKEND), Array(NIAGARA_BACKEND))
    } else {
      // if OS is not linux, only check heap state backend
      Seq[Array[AnyRef]](Array(HEAP_BACKEND))
    }
  }
}
