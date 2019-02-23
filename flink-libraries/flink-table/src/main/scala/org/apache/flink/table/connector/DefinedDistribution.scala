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

package org.apache.flink.table.connector

/**
  * Defines a distribution trait used for operation similar to partitionBy / distributeBy.
  * Can be implemented on a source or a sink.
  */
trait DefinedDistribution {

  def getPartitionFields(): Array[String]

  /**
    * If true, all records would be sort with partition fields before output, for some sinks, this
    * can be used to reduce the partition writers, that means the sink will accept data
    * one partition at a time
    *
    * <p>A sink should consider whether to override this especially when it needs buffer
    * data before writing.
    *
    * Notes:
    * 1. If returns true, the output data will be sorted [locally] after partitioning.
    * 2. Only work if [[getPartitionFields]] return value is non-empty, default to be true.
    */
  def sortLocalPartition(): Boolean = getPartitionFields() != null && getPartitionFields().nonEmpty

  /**
    * If false, all records with empty key will be distributed to single channel.
    * Empty includes all `null` value and empty string.
    */
  def shuffleEmptyKey(): Boolean = false

}
