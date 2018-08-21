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

package org.apache.flink.table.runtime.collector

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}

/**
  * The AsyncCollector is used to wrap left [[BaseRow]] and right [[BaseRow]] to [[JoinedRow]]
  */
class JoinedRowAsyncCollector(
    val leftRow: BaseRow,
    val out: ResultFuture[BaseRow],
    val rightArity: Int,
    val leftOuterJoin: Boolean) extends ResultFuture[BaseRow] {

  val nullRow: GenericRow = new GenericRow(rightArity)

  override def complete(collection: util.Collection[BaseRow]): Unit = {
    if (collection == null || collection.size() == 0) {
      if (leftOuterJoin) {
        val outRow: BaseRow = new JoinedRow(leftRow, nullRow)
        outRow.setHeader(leftRow.getHeader)
        out.complete(Collections.singleton(outRow))
      } else {
        out.complete(Collections.emptyList[BaseRow]())
      }
    } else {
      // TODO: currently, collection should only contain one element
      val rightRow = collection.iterator().next()
      val outRow: BaseRow = new JoinedRow(leftRow, rightRow)
      outRow.setHeader(leftRow.getHeader)
      out.complete(Collections.singleton(outRow))
    }
  }

  override def completeExceptionally(throwable: Throwable): Unit = {
    out.completeExceptionally(throwable)
  }
}
