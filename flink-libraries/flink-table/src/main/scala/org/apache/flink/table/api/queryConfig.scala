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

package org.apache.flink.table.api

import _root_.java.io.Serializable

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.functions.AggregateFunction

class QueryConfig private[table] extends Serializable {
}

object QueryConfig {
  def getQueryConfigFromTableEnv(tableEnv: TableEnvironment): QueryConfig = {
    tableEnv match {
      case s: StreamTableEnvironment => s.queryConfig
      case _ => null
    }
  }
}

/**
  * The [[BatchQueryConfig]] holds parameters to configure the behavior of batch queries.
  */
class BatchQueryConfig private[table] extends QueryConfig

/**
  * The [[StreamQueryConfig]] holds parameters to configure the behavior of streaming queries.
  *
  * An empty [[StreamQueryConfig]] can be generated using the [[StreamTableEnvironment.queryConfig]]
  * method.
  */
class StreamQueryConfig extends QueryConfig {

  private val DEFAULT_FIRE_INTERVAL = Long.MinValue

  /**
    * The minimum time until state which was not updated will be retained.
    * State might be cleared and removed if it was not updated for the defined period of time.
    */
  private var minIdleStateRetentionTime: Long = Long.MinValue

  /**
    * The maximum time until state which was not updated will be retained.
    * State will be cleared and removed if it was not updated for the defined period of time.
    */
  private var maxIdleStateRetentionTime: Long = Long.MinValue

  /**
    * The early firing interval in milli second, early fire is the emit strategy
    * before watermark advanced to end of window.
    *
    * < 0 means no early fire
    * 0 means no delay (fire on every element).
    * > 0 means the fire interval
    */
  private var earlyFireInterval = DEFAULT_FIRE_INTERVAL

  /**
    * The late firing interval in milli second, late fire is the emit strategy
    * after watermark advanced to end of window.
    *
    * < 0 means no late fire, drop every late elements
    * 0 means no delay (fire on every element).
    * > 0 means the fire interval
    *
    * NOTE: late firing strategy is only enabled when allowLateness > 0
    */
  private var lateFireInterval = DEFAULT_FIRE_INTERVAL

  private var microBatchEnabled: Boolean = false

  private var microBatchTriggerTime: Long = Long.MinValue

  private var miniBatchEnabled: Boolean = false

  private var miniBatchTriggerTime: Long = Long.MinValue

  private var miniBatchTriggerSize: Long = Long.MinValue

  private var localAggEnabled: Boolean = false

  private var partialAggEnabled: Boolean = false

  private var miniBatchJoinEnabled: Boolean = false

  private var topnCacheSize: Long = 10000

  private var topnApproxEnabled: Boolean = false

  private var topnApproxBufferMultiplier: Long = 2

  private var topnApproxBufferMinSize: Long = 400

  /**
    * Set whether to enable universal sort for stream.
    * When it is false, universal sort can't use for stream, default false.
    * Just for testing.
    */
  private var nonTemporalSort: Boolean = false

  /**
    * Whether support values source input
    *
    * The reason for disabling this feature is that checkpoint will not work properly when
    * source finished
    */
  private var valuesSourceInputEnabled: Boolean = false

  /**
    * Stores managed table name to aggregate state writer mapping.
    * The key is managed table name, value is aggregate function identifier and function itself.
    */
  private var queryableState2AggFunctionMap: Map[String, JTuple2[String, AggregateFunction[_, _]]] =
    Map.empty

  /**
    * keep track of user defined parameters for each state table.
    */
  var queryableState2ParamMap: Map[String, Map[String, String]] = Map.empty

  /**
    * Specifies the time interval for how long idle state, i.e., state which was not updated, will
    * be retained. When state was not updated for the specified interval of time, it will be cleared
    * and removed.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Note: [[withIdleStateRetentionTime(minTime: Time, maxTime: Time)]] allows to set a minimum and
    * maximum time for state to be retained. This method is more efficient, because the system has
    * to do less bookkeeping to identify the time at which state must be cleared.
    *
    * @param time The time interval for how long idle state is retained. Set to 0 (zero) to never
    *             clean-up the state.
    */
  def withIdleStateRetentionTime(time: Time): StreamQueryConfig = {
    withIdleStateRetentionTime(time, time)
  }

  /**
    * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
    * was not updated, will be retained.
    * State will never be cleared until it was idle for less than the minimum time and will never
    * be kept if it was idle for more than the maximum time.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Set to 0 (zero) to never clean-up the state.
    *
    * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
    *                never clean-up the state.
    * @param maxTime The maximum time interval for which idle state is retained. May not be smaller
    *                than than minTime. Set to 0 (zero) to never clean-up the state.
    */
  def withIdleStateRetentionTime(minTime: Time, maxTime: Time): StreamQueryConfig = {
    if (maxTime.toMilliseconds < minTime.toMilliseconds) {
      throw new IllegalArgumentException("maxTime may not be smaller than minTime.")
    }
    minIdleStateRetentionTime = minTime.toMilliseconds
    maxIdleStateRetentionTime = maxTime.toMilliseconds
    this
  }

  /**
    * Specifies the early firing interval in milli second, early fire is the emit strategy
    * before watermark advanced to end of window.
    */
  def withEarlyFireInterval(interval: Time): StreamQueryConfig = {
    if (this.earlyFireInterval != DEFAULT_FIRE_INTERVAL
      && this.earlyFireInterval != interval.toMilliseconds) {
      // earlyFireInterval of the two query config is not equal and not the default
      throw new RuntimeException(
        "Currently not support different earlyFireInterval configs in one job")
    }
    earlyFireInterval = interval.toMilliseconds
    this
  }

  /**
    * Specifies the late firing interval in milli second, early fire is the emit strategy
    * after watermark advanced to end of window.
    */
  def withLateFireInterval(interval: Time): StreamQueryConfig = {
    if (this.lateFireInterval != DEFAULT_FIRE_INTERVAL
      && this.lateFireInterval != interval.toMilliseconds) {
      // lateFireInterval of the two query config is not equal and not the default
      throw new RuntimeException(
        "Currently not support different lateFireInterval configs in one job")
    }
    lateFireInterval = interval.toMilliseconds
    this
  }

  def getMinIdleStateRetentionTime: Long = {
    minIdleStateRetentionTime
  }

  def getMaxIdleStateRetentionTime: Long = {
    maxIdleStateRetentionTime
  }

  def getEarlyFireInterval: Long = earlyFireInterval

  def getLateFireInterval: Long = lateFireInterval

  def isMiniBatchEnabled: Boolean = miniBatchEnabled

  def enableMiniBatch: StreamQueryConfig = {
    miniBatchEnabled = true
    this
  }

  def disableMiniBatch: StreamQueryConfig = {
    miniBatchEnabled = false
    this
  }

  def isMicroBatchEnabled: Boolean = microBatchEnabled

  def enableMicroBatch: StreamQueryConfig = {
    microBatchEnabled = true
    this
  }

  def disableMicroBatch: StreamQueryConfig = {
    microBatchEnabled = false
    this
  }

  def withMicroBatchTriggerTime(triggerTime: Long): StreamQueryConfig = {
    if (triggerTime <= 0L) {
      throw new RuntimeException("triggerTime must be positive")
    }
    microBatchTriggerTime = triggerTime
    this
  }

  def getMicroBatchTriggerTime: Long = microBatchTriggerTime

  def isLocalAggEnabled: Boolean = localAggEnabled

  def isPartialAggEnabled: Boolean = partialAggEnabled

  def isMiniBatchJoinEnabled: Boolean = miniBatchJoinEnabled

  def isValuesSourceInputEnabled: Boolean = valuesSourceInputEnabled

  def enableLocalAgg: StreamQueryConfig = {
    localAggEnabled = true
    this
  }

  def disableLocalAgg: StreamQueryConfig = {
    localAggEnabled = false
    this
  }

  def enablePartialAgg: StreamQueryConfig = {
    partialAggEnabled = true
    this
  }

  def disablePartialAgg: StreamQueryConfig = {
    partialAggEnabled = false
    this
  }

  def enableMiniBatchJoin: StreamQueryConfig = {
    miniBatchJoinEnabled = true
    this
  }

  def disableMiniBatchJoin: StreamQueryConfig = {
    miniBatchJoinEnabled = false
    this
  }

  def withMiniBatchTriggerTime(triggerTime: Long): StreamQueryConfig = {
    if (triggerTime <= 0L) {
      throw new RuntimeException("triggerTime must be positive")
    }
    miniBatchTriggerTime = triggerTime
    this
  }

  def withMiniBatchTriggerSize(triggerSize: Long): StreamQueryConfig = {
    if (triggerSize <= 0L) {
      throw new RuntimeException("triggerSize must be positive")
    }
    miniBatchTriggerSize = triggerSize
    this
  }

  def getMiniBatchTriggerTime: Long = miniBatchTriggerTime

  def getMiniBatchTriggerSize: Long = miniBatchTriggerSize

  def addQueryableState2AggFunctionMapping(queryableName: String, udagg: JTuple2[String,
    AggregateFunction[_, _]]) = {
    queryableState2AggFunctionMap += (queryableName -> udagg)
    this
  }

  def getAggFunctionByQueryableStateName(queryableName: String): JTuple2[String,
    AggregateFunction[_, _]] = queryableState2AggFunctionMap.get(queryableName).orNull

  def withTopNCacheSize(cacheSize: Long): StreamQueryConfig = {
    if (cacheSize <= 0L) {
      throw new IllegalArgumentException("cacheSize must be positive")
    }
    this.topnCacheSize = cacheSize
    this
  }

  def getTopNCacheSize: Long = topnCacheSize

  def enableTopNApprox: StreamQueryConfig = {
    this.topnApproxEnabled = true
    this
  }

  def disableTopNApprox: StreamQueryConfig = {
    this.topnApproxEnabled = false
    this
  }

  def isTopNApproxEnabled: Boolean = topnApproxEnabled

  def withTopNApproxBufferMultiplier(topnApproxBufferMultiplier: Long): StreamQueryConfig = {
    this.topnApproxBufferMultiplier = topnApproxBufferMultiplier
    this
  }

  def getTopNApproxBufferMultiplier: Long = topnApproxBufferMultiplier

  def withTopNApproxBufferMinSize(topnApproxBufferMinSize: Long): StreamQueryConfig = {
    this.topnApproxBufferMinSize = topnApproxBufferMinSize
    this
  }

  def getTopNApproxBufferMinSize: Long = topnApproxBufferMinSize

  def enableValuesSourceInput: StreamQueryConfig = {
    valuesSourceInputEnabled = true
    this
  }

  def disableValuesSourceInput: StreamQueryConfig = {
    valuesSourceInputEnabled = false
    this
  }

  def enableNonTemporalSort: StreamQueryConfig = {
    nonTemporalSort = true
    this
  }

  def getNonTemporalSort: Boolean = nonTemporalSort

  def copy: StreamQueryConfig = {
    val newCopy = new StreamQueryConfig
    newCopy.minIdleStateRetentionTime = this.minIdleStateRetentionTime
    newCopy.maxIdleStateRetentionTime = this.maxIdleStateRetentionTime
    newCopy.miniBatchEnabled = this.miniBatchEnabled
    newCopy.miniBatchTriggerTime = this.miniBatchTriggerTime
    newCopy.miniBatchTriggerSize = this.miniBatchTriggerSize
    newCopy.localAggEnabled = this.localAggEnabled
    newCopy.miniBatchJoinEnabled = this.miniBatchJoinEnabled
    newCopy.earlyFireInterval = this.earlyFireInterval
    newCopy.lateFireInterval = this.lateFireInterval
    newCopy.topnCacheSize = this.topnCacheSize
    newCopy.topnApproxEnabled = this.topnApproxEnabled
    newCopy.topnApproxBufferMultiplier = this.topnApproxBufferMultiplier
    newCopy.topnApproxBufferMinSize = this.topnApproxBufferMinSize
    newCopy.valuesSourceInputEnabled = this.valuesSourceInputEnabled
    newCopy.nonTemporalSort = this.nonTemporalSort
    newCopy
  }
}
