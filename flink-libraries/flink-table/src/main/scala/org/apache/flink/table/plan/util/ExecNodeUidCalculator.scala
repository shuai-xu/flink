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
package org.apache.flink.table.plan.util

import java.io.{PrintWriter, StringWriter}
import java.util

import org.apache.flink.table.plan.nodes.exec.StreamExecNode
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecSink
import org.apache.flink.table.runtime.functions.utils.Md5Utils

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.mutable

object ExecNodeUidCalculator {

  private val cachedUids = new util.IdentityHashMap[StreamExecNode[_], Option[String]]()

  private val cachedStateDigests = new util.IdentityHashMap[StreamExecNode[_], Option[String]]()

  /**
    * Returns the optional uid of the specified ExecNode, None if the specified
    * exec node has not state.
    */
  def getUid(node: StreamExecNode[_]): Option[String] = {
    cachedUids.getOrElseUpdate(node, {
      getStateDigest(node).map {plainUid =>
        if (node.isInstanceOf[StreamExecSink[_]]) {
          // there is no need to take the inputs into account for sink node
          Md5Utils.md5sum(plainUid)
        } else {
          Md5Utils.md5sum(plainUid + getInputStateDigests(node).mkString(", "))
        }
      }
    })
  }

  /**
    * Returns the state digest of the specified ExecNode itself NOT including its inputs.
    */
  def getStateDigest(node: StreamExecNode[_]): Option[String] = {
    cachedStateDigests.getOrElseUpdate(node, {
      val sw = new StringWriter
      val pw = new PrintWriter(sw)
      val execNodeInfoWriter = new ExecNodeInfoWriter(pw, ExecNodeInfoWriter.STREAM_EXEC, true)
      node.getStateDigest(execNodeInfoWriter).done(node)
      val plainUid = sw.toString
      if (plainUid.isEmpty) {
        None
      } else {
        Some(plainUid)
      }
    })
  }

  /**
    * Returns the state digests of the inputs of the specified ExecNode.
    */
  private def getInputStateDigests(node: StreamExecNode[_]): Array[String] = {
    val inputStateDigests = mutable.ArrayBuffer[String]()
    node.getInputNodes.foreach {
      case input: StreamExecNode[_] =>
        inputStateDigests.appendAll(getInputStateDigests(input))
        getStateDigest(input).foreach(inputStateDigests.append(_))
    }
    inputStateDigests.toArray
  }
}
