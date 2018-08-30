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

package org.apache.flink.table.sinks

import _root_.java.lang.{Boolean => JBool}

import org.apache.flink.table.api._
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.types.Row

/** Defines an external [[TableSink]] to emit a batch [[Table]] for
  * compatible with stream connect plugin.
  *
  * param JTuple2[JBool, Row] JTuple2.f0 is always true, just compatible stream
  * connect output plugin.
  *
  */
trait BatchExecCompatibleStreamTableSink extends TableSink[JTuple2[JBool, Row]] {

  /** Emits the DataStream. */
  def emitBoundedStream (boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_]
}
