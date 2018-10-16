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
import _root_.java.lang.{Boolean => JBoolean, Long => JLong}
import _root_.java.util.{Map => JMap}

import org.apache.flink.annotation.Experimental
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration, GlobalConfiguration}
import org.apache.flink.table.api.StreamQueryConfig._
import org.apache.flink.table.functions.AggregateFunction

@Experimental
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
@Experimental
class BatchQueryConfig private[table] extends QueryConfig

/**
  * The [[StreamQueryConfig]] holds parameters to configure the behavior of streaming queries.
  *
  * An empty [[StreamQueryConfig]] can be generated using the [[StreamTableEnvironment.queryConfig]]
  * method.
  */
@Experimental
class StreamQueryConfig extends QueryConfig {

  private val DEFAULT_FIRE_INTERVAL = Long.MinValue

  /**
    * Defines user-defined configuration
    */
  private val parameters = GlobalConfiguration.loadConfiguration()

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

  private var miniBatchEnabled: Boolean = false


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
    * Returns user-defined configuration
    */
  def getParameters: Configuration = parameters

  /**
    * Sets user-defined configuration
    */
  def setParameters(parameters: Configuration): Unit = {
    this.parameters.addAll(parameters)
  }

  /**
    * Sets user-defined configuration
    */
  def setParameters(parameters: JMap[String, String]): Unit = {
    this.parameters.addAll(parameters)
  }

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
    this.parameters.setLong(BLINK_STATE_TTL_MS, minTime.toMilliseconds)
    this.parameters.setLong(BLINK_STATE_TTL_MAX_MS, maxTime.toMilliseconds)
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
    this.parameters.getLong(BLINK_STATE_TTL_MS)
  }

  def getMaxIdleStateRetentionTime: Long = {
    // only min idle ttl provided.
    if (this.parameters.contains(BLINK_STATE_TTL_MS)
      && !this.parameters.contains(BLINK_STATE_TTL_MAX_MS)) {
      this.parameters.setLong(BLINK_STATE_TTL_MAX_MS, getMinIdleStateRetentionTime * 2)
    }
    this.parameters.getLong(BLINK_STATE_TTL_MAX_MS)
  }

  def getEarlyFireInterval: Long = earlyFireInterval

  def getLateFireInterval: Long = lateFireInterval

  def isMiniBatchEnabled: Boolean = {
    if (this.parameters.contains(BLINK_MINIBATCH_ALLOW_LATENCY)) {
      miniBatchEnabled = true
    }
    miniBatchEnabled
  }

  def enableMiniBatch: StreamQueryConfig = {
    miniBatchEnabled = true
    this
  }

  def disableMiniBatch: StreamQueryConfig = {
    miniBatchEnabled = false
    this
  }

  def isMicroBatchEnabled: Boolean = {
    if (this.parameters.contains(BLINK_MICROBATCH_ALLOW_LATENCY)) {
      microBatchEnabled = true
    }
    microBatchEnabled
  }

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
    this.parameters.setLong(BLINK_MICROBATCH_ALLOW_LATENCY, triggerTime)
    this
  }

  def getMicroBatchTriggerTime: Long = {
    this.parameters.getLong(BLINK_MICROBATCH_ALLOW_LATENCY)
  }

  def isLocalAggEnabled: Boolean = parameters.getBoolean(SQL_EXEC_AGG_LOCAL_ENABLED)

  def isPartialAggEnabled: Boolean = parameters.getBoolean(SQL_EXEC_AGG_PARTIAL_ENABLED)

  def isMiniBatchJoinEnabled: Boolean = {
    // enable miniBatch Join by default if miniBatch is enabled.
    isMiniBatchEnabled && this.parameters.getBoolean(BLINK_MINIBATCH_JOIN_ENABLED)
  }

  def isValuesSourceInputEnabled: Boolean = {
    this.parameters.getBoolean(BLINK_VALUES_SOURCE_INPUT_ENABLED)
  }

  def enableLocalAgg: StreamQueryConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_LOCAL_ENABLED, true)
    this
  }

  def disableLocalAgg: StreamQueryConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_LOCAL_ENABLED, false)
    this
  }

  def enablePartialAgg: StreamQueryConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_PARTIAL_ENABLED, true)
    this
  }

  def disablePartialAgg: StreamQueryConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_PARTIAL_ENABLED, false)
    this
  }

  def enableMiniBatchJoin: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_MINIBATCH_JOIN_ENABLED, true)
    this
  }

  def disableMiniBatchJoin: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_MINIBATCH_JOIN_ENABLED, false)
    this
  }

  def withMiniBatchTriggerTime(triggerTime: Long): StreamQueryConfig = {
    if (triggerTime <= 0L) {
      throw new RuntimeException("triggerTime must be positive")
    }
    this.parameters.setLong(BLINK_MINIBATCH_ALLOW_LATENCY, triggerTime)
    this
  }

  def withMiniBatchTriggerSize(triggerSize: Long): StreamQueryConfig = {
    if (triggerSize <= 0L) {
      throw new RuntimeException("triggerSize must be positive")
    }
    this.parameters.setLong(BLINK_MINIBATCH_SIZE, triggerSize)
    this
  }

  def getMiniBatchTriggerTime: Long = {
    this.parameters.getLong(BLINK_MINIBATCH_ALLOW_LATENCY)
  }

  def getMiniBatchTriggerSize: Long = {
    this.parameters.getLong(BLINK_MINIBATCH_SIZE)
  }

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
    this.parameters.setLong(BLINK_TOPN_CACHE_SIZE, cacheSize)
    this
  }

  def getTopNCacheSize: Long = {
    this.parameters.getLong(BLINK_TOPN_CACHE_SIZE)
  }

  def enableTopNApprox: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_TOPN_APPROXIMATE_ENABLED, true)
    this
  }

  def disableTopNApprox: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_TOPN_APPROXIMATE_ENABLED, false)
    this
  }

  def isTopNApproxEnabled: Boolean = {
    this.parameters.getBoolean(BLINK_TOPN_APPROXIMATE_ENABLED)
  }

  def withTopNApproxBufferMultiplier(topnApproxBufferMultiplier: Long): StreamQueryConfig = {
    if (topnApproxBufferMultiplier <= 0L) {
      throw new IllegalArgumentException("topnApproxBufferMultiplier must be positive")
    }
    this.parameters.setLong(BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER, topnApproxBufferMultiplier)
    this
  }

  def getTopNApproxBufferMultiplier: Long = {
    this.parameters.getLong(BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER)
  }

  def withTopNApproxBufferMinSize(topnApproxBufferMinSize: Long): StreamQueryConfig = {
    if (topnApproxBufferMinSize < 0L) {
      throw new IllegalArgumentException("topnApproxBufferMinSize must be positive")
    }
    this.parameters.setLong(BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, topnApproxBufferMinSize)
    this
  }

  def getTopNApproxBufferMinSize: Long = {
    this.parameters.getLong(BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE)
  }

  def enableValuesSourceInput: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_VALUES_SOURCE_INPUT_ENABLED, true)
    this
  }

  def disableValuesSourceInput: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_VALUES_SOURCE_INPUT_ENABLED, false)
    this
  }

  def enableNonTemporalSort: StreamQueryConfig = {
    this.parameters.setBoolean(BLINK_NON_TEMPORAL_SORT_ENABLED, true)
    this
  }

  def isNonTemporalSortEnabled: Boolean = {
    this.parameters.getBoolean(BLINK_NON_TEMPORAL_SORT_ENABLED)
  }

  def withPartialBucketNum(buckets: Integer): StreamQueryConfig = {
    this.parameters.setInteger(SQL_EXEC_AGG_PARTIAL_BUCKET_NUM, buckets)
    this
  }

  def getPartialBucketNum: Integer = {
    val bucketNum = this.parameters.getInteger(SQL_EXEC_AGG_PARTIAL_BUCKET_NUM)
    if (bucketNum <= 0) {
      throw new RuntimeException("Bucket number in Partial Agg must be positive!")
    }
    bucketNum
  }
}

object StreamQueryConfig {

  /** configure number of buckets in partial final mode */
  val SQL_EXEC_AGG_PARTIAL_BUCKET_NUM: ConfigOption[Integer] = ConfigOptions
    .key("sql.exec.partialAgg.bucket.num")
    .defaultValue(256)

  /** microbatch allow latency (ms) */
  val BLINK_MICROBATCH_ALLOW_LATENCY: ConfigOption[JLong] = ConfigOptions
    .key("blink.microBatch.allowLatencyMs")
    .defaultValue(Long.MinValue)

  /** minibatch allow latency(ms) */
  val BLINK_MINIBATCH_ALLOW_LATENCY: ConfigOption[JLong] = ConfigOptions
    .key("blink.miniBatch.allowLatencyMs")
    .defaultValue(Long.MinValue)

  /** minibatch size, default 100 */
  val BLINK_MINIBATCH_SIZE: ConfigOption[JLong] = ConfigOptions
    .key("blink.miniBatch.size")
    .defaultValue(Long.MinValue)

  /** whether to enable local agg */
  val SQL_EXEC_AGG_LOCAL_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.localAgg.enabled")
    .defaultValue(true)

  /** whether to enable partial agg */
  val SQL_EXEC_AGG_PARTIAL_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.partialAgg.enabled")
    .defaultValue(false)

  /** whether to enable miniBatch join */
  val BLINK_MINIBATCH_JOIN_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.miniBatch.join.enabled")
    .defaultValue(true)

  /** switch on/off topn approximate update rank operator, default is false */
  val BLINK_TOPN_APPROXIMATE_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.topn.approximate.enabled")
    .defaultValue(false)

  /** cache size of every topn task, default is 10000 */
  val BLINK_TOPN_CACHE_SIZE: ConfigOption[JLong] = ConfigOptions
    .key("blink.topn.cache.size")
    .defaultValue(10000L)

  /** in-memory sort map size multiplier (x2, for example) for topn update rank
    * when approximation is enabled, default is 2. NOTE, We should make sure
    * sort map size limit * blink.topn.approximate.buffer.multiplier < blink.topn.cache.size.
    */
  val BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER: ConfigOption[JLong] = ConfigOptions
    .key("blink.topn.approximate.buffer.multiplier")
    .defaultValue(2L)

  /** in-memory sort map size low minimal size. default is 400, and 0 meaning no low limit
    * for each topn job, if buffer.multiplier * topn < buffer.minsize, then buffer is set
    * to buffer.minsize.
    */
  val BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE: ConfigOption[JLong] = ConfigOptions
    .key("blink.topn.approximate.buffer.minsize")
    .defaultValue(400L)

  /**
    * Whether support values source input
    *
    * The reason for disabling this feature is that checkpoint will not work properly when
    * source finished
    */
  val BLINK_VALUES_SOURCE_INPUT_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.values.source.input.enabled")
    .defaultValue(false)

  /** switch on/off stream sort without temporal or limit
    * Set whether to enable universal sort for stream.
    * When it is false, universal sort can't use for stream, default false.
    * Just for testing.
    */
  val BLINK_NON_TEMPORAL_SORT_ENABLED: ConfigOption[JBoolean] = ConfigOptions
    .key("blink.non-temporal-sort.enabled")
    .defaultValue(false)

  /**
    * The minimum time until state which was not updated will be retained.
    * State might be cleared and removed if it was not updated for the defined period of time.
    */
  val BLINK_STATE_TTL_MS: ConfigOption[JLong] = ConfigOptions
    .key("blink.state.ttl.ms")
    .defaultValue(Long.MinValue)

  /**
    * The maximum time until state which was not updated will be retained.
    * State will be cleared and removed if it was not updated for the defined period of time.
    */
  val BLINK_STATE_TTL_MAX_MS: ConfigOption[JLong] = ConfigOptions
    .key("blink.state.ttl.max.ms")
    .defaultValue(Long.MinValue)
}
