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

import org.apache.flink.table.api.TableException

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
  * Rewrite reusable sub-plans with different rel objects to same rel object.
  *
  * <p>e.g.
  * Scan1-Project1 and Scan2-Project2 have same digest, so they can be reused.
  * {{{
  *      Join                       Join
  *     /    \                     /    \
  * Filter1 Filter2            Filter1 Filter2
  *    |      |          =>        \      /
  * Project1 Project2              Project1
  *    |      |                       |
  *  Scan1   Scan2                  Scan1
  * }}}
  * After rewrote, Scan2-Project2 is replaced by Scan1-Project1.
  *
  * <p>NOTES: This is a stateful class, please use same shuttle object to reuse multiple trees.
  */
class SubplanReuseShuttle(context: SubplanReuseContext) extends DefaultRelShuttle {
  private val mapDigestToNewNode = new util.HashMap[String, RelNode]()

  override def visit(rel: RelNode): RelNode = {
    val canReuseOtherNode = context.reuseOtherNode(rel)
    val digest = context.getRelDigest(rel)
    if (canReuseOtherNode) {
      val newNode = mapDigestToNewNode.get(digest)
      if (newNode == null) {
        throw new TableException("This should not happen")
      }
      newNode
    } else {
      val newNode = visitInputs(rel)
      mapDigestToNewNode.put(digest, newNode)
      newNode
    }
  }

  /**
    * Parent rel need not change, just replaces inputs if any inputs change.
    */
  private def visitInputs(rel: RelNode): RelNode = {
    rel.getInputs.zipWithIndex.foreach {
      case (input, index) =>
        val newInput = input.accept(this)
        if (input ne newInput) {
          rel.replaceInput(index, newInput)
        }
    }
    rel
  }
}
