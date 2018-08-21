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

import org.apache.calcite.rel.RelWriter

import scala.collection.mutable.ArrayBuffer

sealed trait RankLimit extends Serializable {
  def toString(inputFieldNames: Seq[String]): String
  def explain(pw: RelWriter, inputFieldNames: Seq[String]): RelWriter
}

// rankStart and rankEnd are inclusive, rankStart start from one.
case class ConstantRankLimit(rankStart: Long, rankEnd: Long) extends RankLimit {
  override def toString(inputFieldNames: Seq[String]): String = {
    val offset: Long = rankStart - 1
    val fetch: Long = rankEnd - rankStart + 1
    var result: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (rankEnd != Long.MaxValue) {
      result += s", fetch: $fetch)"
    }
    if (offset > 0) {
      result += s", offset: $offset"
    }
    result.mkString(", ")
  }

  override def explain(pw: RelWriter, inputFieldNames: Seq[String]): RelWriter = {
    val offset: Long = rankStart - 1
    val fetch: Long = rankEnd - rankStart + 1
    pw.itemIf("offset", offset, offset > 0)
      .itemIf("fetch", fetch, rankEnd != Long.MaxValue)
  }
}

// changing rank limit depends on input
case class VariableRankLimit(limitField: Int) extends RankLimit {
  override def toString(inputFieldNames: Seq[String]): String = {
    val limitFieldName = inputFieldNames(limitField)
    s"rownum: $limitFieldName"
  }

  override def explain(pw: RelWriter, inputFieldNames: Seq[String]): RelWriter = {
    val limitFieldName = inputFieldNames(limitField)
    pw.item("rownum", limitFieldName)
  }
}
