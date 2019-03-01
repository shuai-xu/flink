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

import org.apache.flink.table.sinks.OperationType.OperationType

/**
  * Adds support for [[TableSink]] to update or delete records.
  * A [[TableSink]] extending this interface is able to update or delete records.
  */
trait UpdateDeleteTableSink {

  /**
    * Operation type, if it is null, this [[TableSink]] inserts records as usually.
    */
  var operationType: OperationType = OperationType.INSERT

  /**
    * PrimaryKeys of the sink table.
    */
  var primaryKeys: Array[String] = _

  /**
    * Sets operation type, set be [[org.apache.flink.table.api.TableEnvironment]]
    */
  private[flink] final def setOperationType(operationType: OperationType): Unit = {
    this.operationType = operationType
  }

  /**
    * Set primaryKeys of the sink table, set by users.
    */
  final def setPrimaryKeys(primaryKeys: Array[String]): Unit = {
    this.primaryKeys = primaryKeys
  }
}

/**
  * Operation type of the [[TableSink]], supporting further update and merge.
  */
object OperationType extends Enumeration {
  type OperationType = Value
  val DELETE, INSERT = Value
}
