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

import _root_.java.util.TimeZone

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.lang.{Long => JLong}
import _root_.java.lang.{Double => JDouble}
import _root_.java.lang.{String => JString}
import org.apache.flink.configuration.ConfigOptions.key

import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration, GlobalConfiguration}
import org.apache.flink.table.api.OperatorType.OperatorType
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.util.StringUtils
import org.apache.flink.table.api.TableConfig._
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.codegen.JavaSourceManipulator

/**
  * A config to define the runtime behavior of the Table API.
  */
class TableConfig {

  /**
    * Defines the timezone for date/time/timestamp conversions.
    */
  private var timeZone: TimeZone = TimeZone.getTimeZone("UTC")

  /**
    * Defines if all fields need to be checked for NULL first.
    */
  private var nullCheck: Boolean = true

  /**
    * Defines the configuration of Calcite for Table API and SQL queries.
    */
  private var calciteConfig = CalciteConfig.createBuilder().build()

  /**
    * Defines whether supports subsection optimization
    */
  private var subsectionOptimization: Boolean = false

  /**
    * Defines user-defined configuration
    */
  private var parameters = GlobalConfiguration.loadConfiguration()

  /**
    * Returns operatorMetricCollect. If enabled, the  operator metric need to be collected during
    * runtime phase.
    */
  def getOperatorMetricCollect: Boolean = parameters.getBoolean(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED)

  /**
    * Sets the operatorMetricCollect.If enabled, the operator metrics need to be collected
    * during runtime phase.
    */
  def setOperatorMetricCollect(operatorMetricCollect: Boolean): Unit = parameters.setBoolean(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED, operatorMetricCollect)

  /**
    * Returns optimizedPlanCollect. If enabled, the optimized plan need to be collected.
    */
  def getOptimizedPlanCollect: Boolean = parameters.getBoolean(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED)

  /**
    * Sets the optimizedPlanCollect.If enabled, the optimized plan need to be collected.
    */
  def setOptimizedPlanCollect(optimizedPlanCollect: Boolean): Unit = parameters.setBoolean(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED, optimizedPlanCollect)

  /**
    * Returns the file path to dump stream graph plan with collected operator metrics.
    */
  def getDumpFileOfPlanWithMetrics: String = parameters.getString(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH)

  /**
    * Sets the file path to dump stream graph plan with collected operator metrics.
    */
  def setDumpFileOfPlanWithMetrics(dumpFileOfPlanWithMetrics: String): Unit = parameters.setString(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH, dumpFileOfPlanWithMetrics)

  /**
    * Returns the file path to dump optimized plan.
    */
  def getDumpFileOfOptimizedPlan: String = parameters.getString(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH)

  /**
    * Sets the file path to dump optimized plan.
    */
  def setDumpFileOfOptimizedPlan(dumpFileOfOptimizedPlan: String): Unit = parameters.setString(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH, dumpFileOfOptimizedPlan)

  /**
    * Returns true if sub-plan reuse is enabled, else false.
    */
  def getSubPlanReuse: Boolean = parameters.getBoolean(
    SQL_EXEC_REUSE_SUB_PLAN_ENABLED)

  /**
    * Sets whether sub-plan reuse is enabled.
    */
  def setSubPlanReuse(reuseSubPlan: Boolean): Unit = parameters.setBoolean(
    SQL_EXEC_REUSE_SUB_PLAN_ENABLED, reuseSubPlan)

  /**
    * Returns true if table-source reuse is disabled, else false.
    */
  def isTableSourceReuseDisabled: Boolean = !parameters.getBoolean(
    SQL_EXEC_REUSE_TABLE_SOURCE_ENABLED)

  /**
    * Sets whether table-source reuse is enabled.
    * This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    */
  def setTableSourceReuse(reuseSubPlan: Boolean): Unit = parameters.setBoolean(
    SQL_EXEC_REUSE_TABLE_SOURCE_ENABLED, reuseSubPlan)

  /**
    * Returns true if nondeterministic-operator reuse is enabled, else false.
    */
  def getNondeterministicOperatorReuse: Boolean = parameters.getBoolean(
    SQL_EXEC_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED)

  /**
    * Sets whether nondeterministic-operator reuse is enabled.
    * This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    */
  def setNondeterministicOperatorReuse(reuseNondeterministicOperator: Boolean): Unit =
    parameters.setBoolean(SQL_EXEC_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED,
      reuseNondeterministicOperator)

  /**
    * Sets the timezone for date/time/timestamp conversions.
    */
  def setTimeZone(timeZone: TimeZone): Unit = {
    require(timeZone != null, "timeZone must not be null.")
    this.timeZone = timeZone

    // Hack, for Timestamp Literal, constants folding
    org.apache.calcite.avatica.util.DateTimeUtils.setUserZone(timeZone)
  }

  /**
    * Returns the timezone for date/time/timestamp conversions.
    */
  def getTimeZone: TimeZone = timeZone

  /**
    * Returns the NULL check. If enabled, all fields need to be checked for NULL first.
    */
  def getNullCheck: Boolean = nullCheck

  /**
    * Sets the NULL check. If enabled, all fields need to be checked for NULL first.
    */
  def setNullCheck(nullCheck: Boolean): Unit = {
    this.nullCheck = nullCheck
  }

  /**
    * Returns the current configuration of Calcite for Table API and SQL queries.
    */
  def getCalciteConfig: CalciteConfig = calciteConfig

  /**
    * Sets the configuration of Calcite for Table API and SQL queries.
    * Changing the configuration has no effect after the first query has been defined.
    */
  def setCalciteConfig(calciteConfig: CalciteConfig): Unit = {
    this.calciteConfig = calciteConfig
  }

  /**
    * Returns user-defined configuration
    */
  def getParameters: Configuration = parameters

  /**
    * Sets user-defined configuration
    */
  def setParameters(parameters: Configuration): Unit = {
    this.parameters = GlobalConfiguration.loadConfiguration()
    this.parameters.addAll(parameters)
  }

  /**
    * Returns whether join reorder is enabled.
    */
  def joinReorderEnabled: Boolean = {
    parameters.getBoolean(SQL_CBO_JOIN_REORDER_ENABLED)
  }

  /**
    * Sets join reorder enabled.
    */
  def setJoinReorderEnabled(joinReorderEnabled: Boolean): Unit = {
    parameters.setBoolean(SQL_CBO_JOIN_REORDER_ENABLED, joinReorderEnabled)
  }

  /**
    * Returns whether join shuffle by partial join keys.
    */
  def joinShuffleByPartialKeyEnabled: Boolean = {
    parameters.getBoolean(SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED)
  }

  /**
    * Sets join shuffle by partial group keys enabled.
    */
  def setJoinShuffleByPartialKeyEnabled(shuffleByPartialKey: Boolean): Unit = {
    parameters.setBoolean(SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED, shuffleByPartialKey)
  }

  /**
    * Returns whether aggregate shuffle by partial group keys.
    */
  def aggregateShuffleByPartialKeyEnabled: Boolean = {
    parameters.getBoolean(SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED)
  }

  /**
    * Sets aggregate shuffle by partial group keys enabled.
    */
  def setAggregateShuffleByPartialKeyEnabled(shuffleByPartialKey: Boolean): Unit = {
    parameters.setBoolean(SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED, shuffleByPartialKey)
  }

  /**
    * Returns whether rank shuffle by partial partition keys.
    */
  def rankShuffleByPartialKeyEnabled: Boolean = {
    parameters.getBoolean(SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED)
  }

  /**
    * Sets rank shuffle by partial partition keys enabled.
    */
  def setRankShuffleByPartialKeyEnabled(shuffleByPartialKey: Boolean): Unit = {
    parameters.setBoolean(SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED, shuffleByPartialKey)
  }

  /**
    * Returns whether supports subsection optimization.
    */
  def getSubsectionOptimization: Boolean = subsectionOptimization

  /**
    * Sets the subsection optimization. If enabled, we will reuse DataSet/DataSteam plan as much
    * as possible. The whole plan will be decomposed into multiple blocks, and each block will be
    * optimized alone.
    */
  def setSubsectionOptimization(subsectionOptimization: Boolean): Unit = {
    this.subsectionOptimization = subsectionOptimization
  }

  /**
    * Returns whether union all is disabled as breakpoint in subsection optimization.
    */
  def isUnionAllAsBreakPointInSubsectionOptimizationDisabled: Boolean =
    parameters.getBoolean(SQL_SUBSECTION_OPTIMIZATION_UNIONALL_AS_BREAKPOINT_DISABLED)

  /**
    * Sets whether to forbid union all as breakpoint in subsection optimization. If flag is true,
    * does not create new [[org.apache.flink.table.plan.RelNodeBlock]] at union node even if it
    * has multiple parents, create new [[org.apache.flink.table.plan.RelNodeBlock]] at input
    * nodes of union node which has multiple parents.
    */
  def disableUnionAllAsBreakPointInSubsectionOptimization(flag: Boolean): Unit = {
    parameters.setBoolean(SQL_SUBSECTION_OPTIMIZATION_UNIONALL_AS_BREAKPOINT_DISABLED, flag)
  }

  def enabledGivenOpType(operator: OperatorType): Boolean = {
    val disableOperators = parameters.getString(SQL_PHYSICAL_OPERATORS_DISABLED)
        .split(",")
        .map(_.trim)
    if (disableOperators.contains("HashJoin") &&
        (operator == OperatorType.BroadcastHashJoin ||
            operator == OperatorType.ShuffleHashJoin)) {
      false
    } else {
      !disableOperators.contains(operator.toString)
    }
  }

  /**
    * @return current maxGeneratedCodeLength
    */
  def getMaxGeneratedCodeLength: Int =
    parameters.getInteger(SQL_CODEGEN_MAX_LENGTH)

  /**
    * set value to 'sql.codegen.maxLength',
    * generated code will be split if code length exceeds this limitation
    */
  def setMaxGeneratedCodeLength(maxGeneratedCodeLength: Int): Unit = {
    parameters.setInteger(SQL_CODEGEN_MAX_LENGTH, maxGeneratedCodeLength)
  }

  /**
    * @return code rewrite enabled
    */
  def codegenRewriteEnabled: Boolean =
    JavaSourceManipulator.isRewriteEnable


  /**
    * set value to 'sql.codegen.rewrite'
    * generated code will be rewrite to avoid JVM limitations.
    */
  def setCodegenRewriteEnabled(codegenRewriteEnabled: Boolean): Unit = {
    JavaSourceManipulator.setRewriteEnable(codegenRewriteEnabled)
  }

  /**
    * @return value of 'sql.codegen.rewrite.maxMethodLength'
    */
  def getCodegenRewriteMaxMethodLength: Long =
    JavaSourceManipulator.getMaxMethodLengthForJvm

  /**
    * set value to 'sql.codegen.rewrite.maxMethodLength'
    */
  def setCodegenRewriteMaxMethodLength(maxLength: Long): Unit = {
    JavaSourceManipulator.setMaxMethodLengthForJvm(maxLength)
  }

  /**
    * @return value of 'sql.codegen.rewrite.maxMethodLengthAfterSplit'
    */
  def getCodegenRewriteMaxMethodLengthAfterSplit: Long =
    JavaSourceManipulator.getMaxMethodLengthForJit

  /**
    * set value to 'sql.codegen.rewrite.maxMethodLengthAfterSplit'
    */
  def setCodegenRewriteMaxMethodLengthAfterSplit(maxLength: Long): Unit = {
    JavaSourceManipulator.setMaxMethodLengthForJit(maxLength)
  }

  /**
    * @return value of 'sql.codegen.rewrite.maxFieldCount'
    */
  def getCodegenRewriteMaxFieldCount: Int =
    JavaSourceManipulator.getMaxFieldCount

  /**
    * set value to 'sql.codegen.rewrite.maxFieldCount'
    */
  def setCodegenRewriteMaxFieldCount(maxFieldCount: Int): Unit = {
    JavaSourceManipulator.setMaxFieldCount(maxFieldCount)
  }

  /**
    * enable code generate debug for janino
    * like "gcc -g"
    */
  def enableCodeGenerateDebug: TableConfig = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    this
  }

  def disableCodeGenerateDebug: TableConfig = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "false")
    this
  }

  def setCodeGenerateTmpDir(path: String): Unit = {
    if (!StringUtils.isNullOrWhitespaceOnly(path)) {
      System.setProperty("org.codehaus.janino.source_debugging.dir", path)
    } else {
      throw new RuntimeException("code generate tmp dir can't be empty")
    }
  }

  def isAllDataExchangeModeBatch: Boolean = {
    parameters.getBoolean(SQL_EXEC_ALL_DATA_EXCHANGE_MODE_BATCH)
  }

  def enableRangePartition: Boolean = {
    parameters.getBoolean(SQL_EXEC_SORT_ENABLE_RANGE)
  }

  /** Return max cnf node limit */
  def getMaxCnfNodeCount: Int = {
    parameters.getInteger(SQL_CBO_CNF_NODES_LIMIT)
  }

  private val DEFAULT_FIRE_INTERVAL = Long.MinValue

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
  def withIdleStateRetentionTime(time: Time): TableConfig = {
    withIdleStateRetentionTime(time, time)
    this
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
  def withIdleStateRetentionTime(minTime: Time, maxTime: Time): TableConfig = {
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
  def withEarlyFireInterval(interval: Time): TableConfig = {
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
  def withLateFireInterval(interval: Time): TableConfig = {
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

  def enableMiniBatch: TableConfig = {
    miniBatchEnabled = true
    this
  }

  def disableMiniBatch: TableConfig = {
    miniBatchEnabled = false
    this
  }

  def isMicroBatchEnabled: Boolean = {
    if (this.parameters.contains(BLINK_MICROBATCH_ALLOW_LATENCY)) {
      microBatchEnabled = true
    }
    microBatchEnabled
  }

  def enableMicroBatch: TableConfig = {
    microBatchEnabled = true
    this
  }

  def disableMicroBatch: TableConfig = {
    microBatchEnabled = false
    this
  }

  def withMicroBatchTriggerTime(triggerTime: Long): TableConfig = {
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

  def enableLocalAgg: TableConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_LOCAL_ENABLED, true)
    this
  }

  def disableLocalAgg: TableConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_LOCAL_ENABLED, false)
    this
  }

  def enablePartialAgg: TableConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_PARTIAL_ENABLED, true)
    this
  }

  def disablePartialAgg: TableConfig = {
    this.parameters.setBoolean(SQL_EXEC_AGG_PARTIAL_ENABLED, false)
    this
  }

  def enableIncrementalAgg: TableConfig = {
    parameters.setBoolean(SQL_EXEC_AGG_INCREMENTAL_ENABLED, true)
    this
  }

  def disableIncrementalAgg: TableConfig = {
    parameters.setBoolean(SQL_EXEC_AGG_INCREMENTAL_ENABLED, false)
    this
  }

  def enableMiniBatchJoin: TableConfig = {
    this.parameters.setBoolean(BLINK_MINIBATCH_JOIN_ENABLED, true)
    this
  }

  def disableMiniBatchJoin: TableConfig = {
    this.parameters.setBoolean(BLINK_MINIBATCH_JOIN_ENABLED, false)
    this
  }

  def withMiniBatchTriggerTime(triggerTime: Long): TableConfig = {
    if (triggerTime <= 0L) {
      throw new RuntimeException("triggerTime must be positive")
    }
    this.parameters.setLong(BLINK_MINIBATCH_ALLOW_LATENCY, triggerTime)
    this
  }

  def withMiniBatchTriggerSize(triggerSize: Long): TableConfig = {
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
      AggregateFunction[_, _]]): TableConfig = {
    queryableState2AggFunctionMap += (queryableName -> udagg)
    this
  }

  def getAggFunctionByQueryableStateName(queryableName: String): JTuple2[String,
      AggregateFunction[_, _]] = queryableState2AggFunctionMap.get(queryableName).orNull

  def withTopNCacheSize(cacheSize: Long): TableConfig = {
    if (cacheSize <= 0L) {
      throw new IllegalArgumentException("cacheSize must be positive")
    }
    this.parameters.setLong(BLINK_TOPN_CACHE_SIZE, cacheSize)
    this
  }

  def getTopNCacheSize: Long = {
    this.parameters.getLong(BLINK_TOPN_CACHE_SIZE)
  }

  def enableTopNApprox: TableConfig = {
    this.parameters.setBoolean(BLINK_TOPN_APPROXIMATE_ENABLED, true)
    this
  }

  def disableTopNApprox: TableConfig = {
    this.parameters.setBoolean(BLINK_TOPN_APPROXIMATE_ENABLED, false)
    this
  }

  def isTopNApproxEnabled: Boolean = {
    this.parameters.getBoolean(BLINK_TOPN_APPROXIMATE_ENABLED)
  }

  def withTopNApproxBufferMultiplier(topnApproxBufferMultiplier: Long): TableConfig = {
    if (topnApproxBufferMultiplier <= 0L) {
      throw new IllegalArgumentException("topnApproxBufferMultiplier must be positive")
    }
    this.parameters.setLong(BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER, topnApproxBufferMultiplier)
    this
  }

  def getTopNApproxBufferMultiplier: Long = {
    this.parameters.getLong(BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER)
  }

  def withTopNApproxBufferMinSize(topnApproxBufferMinSize: Long): TableConfig = {
    if (topnApproxBufferMinSize < 0L) {
      throw new IllegalArgumentException("topnApproxBufferMinSize must be positive")
    }
    this.parameters.setLong(BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE, topnApproxBufferMinSize)
    this
  }

  def getTopNApproxBufferMinSize: Long = {
    this.parameters.getLong(BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE)
  }

  def enableValuesSourceInput: TableConfig = {
    this.parameters.setBoolean(BLINK_VALUES_SOURCE_INPUT_ENABLED, true)
    this
  }

  def disableValuesSourceInput: TableConfig = {
    this.parameters.setBoolean(BLINK_VALUES_SOURCE_INPUT_ENABLED, false)
    this
  }

  def enableNonTemporalSort: TableConfig = {
    this.parameters.setBoolean(BLINK_NON_TEMPORAL_SORT_ENABLED, true)
    this
  }

  def isNonTemporalSortEnabled: Boolean = {
    this.parameters.getBoolean(BLINK_NON_TEMPORAL_SORT_ENABLED)
  }

  def withPartialBucketNum(buckets: Integer): TableConfig = {
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

object OperatorType extends Enumeration {
  type OperatorType = Value
  val NestedLoopJoin, ShuffleHashJoin, BroadcastHashJoin, SortMergeJoin, HashAgg, SortAgg = Value
}

object AggPhaseEnforcer extends Enumeration {
  type AggPhaseEnforcer = Value
  val NONE, ONE_PHASE, TWO_PHASE = Value
}

object TableConfig {
  def DEFAULT = new TableConfig()

  // =================================== Sort ===================================
  val SQL_EXEC_SORT_ENABLE_RANGE: ConfigOption[JBoolean] =
    key("sql.exec.sort.enable-range")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Sets whether to enable range sort, use range sort to sort all " +
            "data in several partitions. When it is false, sorting in only one partition")

  val SQL_EXEC_SORT_DEFAULT_LIMIT: ConfigOption[Integer] =
    key("sql.exec.sort.default-limit")
      .defaultValue(new Integer(200))
      .withDescription("Default limit when user don't set a limit after order by. " +
            "This default value will be invalidated if [[SQL_EXEC_SORT_ENABLE_RANGE]] " +
            "is set to be true.")

  val SQL_EXEC_SORT_BUFFER_MEM: ConfigOption[Integer] =
    key("sql.exec.sort.buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the buffer reserved memory size for sort. " +
          "It defines the lower limit for the sort.")

  val SQL_EXEC_SORT_PREFER_BUFFER_MEM: ConfigOption[Integer] =
    key("sql.exec.sort.prefer-buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the preferred buffer memory size for sort. " +
          "It defines the applied memory for the sort.")

  val SQL_EXEC_SORT_MAX_BUFFER_MEM: ConfigOption[Integer] =
    key("sql.exec.sort.max-buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the max buffer memory size for sort. " +
          "It defines the upper memory for the sort.")

  val SQL_EXEC_SORT_MAX_NUM_FILE_HANDLES: ConfigOption[Integer] =
    key("sql.exec.sort.max-num-file-handles")
      .defaultValue(new Integer(128))
      .withDescription(
          "Sort merge's maximum number of roads, too many roads, may cause too many files to be" +
              " read at the same time, resulting in excessive use of memory.")

  val SQL_EXEC_SPILL_COMPRESSION_ENABLE: ConfigOption[JBoolean] =
    key("sql.exec.spill.compression.enable")
      .defaultValue(JBoolean.TRUE)

  val SQL_EXEC_SPILL_COMPRESSION_CODEC: ConfigOption[String] =
    key("sql.exec.spill.compression.codec")
      .defaultValue("lz4")

  val SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE: ConfigOption[Integer] =
    key("sql.exec.spill.compression.block-size")
      .defaultValue(new Integer(64 * 1024))

  val SQL_EXEC_SORT_ASYNC_MERGE_ENABLE: ConfigOption[JBoolean] =
    key("sql.exec.sort.async-merge.enable")
      .defaultValue(JBoolean.TRUE)

  // =================================== Join ===================================
  val SQL_EXEC_HASH_JOIN_TABLE_MEM: ConfigOption[Integer] =
    key("sql.exec.hash-join.table-memory-mb")
      .defaultValue(new Integer(512))
      .withDescription("Sets the HashTable reserved memory for hashJoin operator." +
            "It defines the lower limit for.")

  val SQL_HASH_JOIN_BROADCAST_THRESHOLD: ConfigOption[JLong] =
    key("sql.exec.hash-join.broadcast-threshold")
      .defaultValue(new JLong(1 * 1024 * 1024))
      .withDescription("Maximum size in bytes for data that could be broadcast to each parallel" +
            " instance that holds a partition of all data when performing a hash join. " +
            "Broadcast will be disabled if the value is -1.")

  // =================================== Aggregate ===================================

  val SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE: ConfigOption[Integer] =
    key("sql.exec.window-agg.buffer-limit-size")
      .defaultValue(new Integer(100 * 1000))
      .withDescription("Sets the window elements buffer limit in size used in" +
            " group window agg operator.")

  val SQL_EXEC_HASH_AGG_TABLE_MEM: ConfigOption[Integer] =
    key("sql.exec.hash-agg.table-memory-mb")
      .defaultValue(new Integer(128))
      .withDescription("Sets the table reserved memory size of hashAgg operator." +
            "It defines the lower limit.")

  val SQL_EXEC_AGG_GROUPS_NDV_RATIO: ConfigOption[JDouble] =
    key("sql.exec.agg.groups.ndv.ratio")
      .defaultValue(new JDouble(0.8))
      .withDescription("Sets the ratio of aggregation. If outputRowCount/inputRowCount of an " +
            "aggregate is less than this ratio, HashAggregate would be thought " +
            "better than SortAggregate.")

  val SQL_EXEC_SEMI_BUILD_DISTINCT_NDV_RATIO: ConfigOption[JDouble] =
    key("sql.exec.semi.build.dictinct.ndv.ratio")
      .defaultValue(new JDouble(0.8))

  // =================================== Buffer ===================================

  val SQL_EXEC_EXTERNAL_BUFFER_MEM: ConfigOption[Integer] =
    key("sql.exec.external-buffer.memory-mb")
      .defaultValue(new Integer(128))
      .withDescription("Sets the externalBuffer memory size that is used in " +
            "sortMergeJoin and overWindow.")

  // =================================== Source ===================================
  val SQL_EXEC_SOURCE_MEM: ConfigOption[Integer] =
    key("sql.exec.source.default-memory-mb")
      .defaultValue(new Integer(128))
      .withDescription("Sets the heap memory size of source operator.")

  // TODO [[ConfigOption]] need to support no default int value
  val SQL_EXEC_SOURCE_PARALLELISM: ConfigOption[Integer] =
    key("sql.exec.source.parallelism")
      .defaultValue(new Integer(-1))
      .withDescription("Sets source parallelism if [[SQL_EXEC_INFER_RESOURCE_MODE]] is NONE." +
            "If it is not set, use [[SQL_EXEC_DEFAULT_PARALLELISM]] to set source parallelism.")

  // =================================== resource ================================

  val SQL_EXEC_INFER_RESOURCE_MODE: ConfigOption[JString] =
    key("sql.exec.infer-resource.mode")
      .defaultValue("NONE")
      .withDescription("Sets infer resource mode according to statics." +
            "Only NONE, ONLY_SOURCE or ALL can be set." +
            "If set NONE, parallelism and memory of all node are set by config." +
            "If set ONLY_SOURCE, only source parallelism is inferred according to statics." +
            "If set ALL, parallelism and memory of all node are inferred according to statics.")

  // TODO [[ConfigOption]] need to support no default int value
  val SQL_EXEC_DEFAULT_PARALLELISM: ConfigOption[Integer] =
    key("sql.exec.default-parallelism")
      .defaultValue(new Integer(-1))
      .withDescription("Default parallelism of the job. If any node do not have special " +
            "parallelism, use it. Its default value is the num of cpu cores in the client host.")

  val SQL_EXEC_DEFAULT_CPU: ConfigOption[JDouble] =
    key("sql.exec.default-cpu")
      .defaultValue(new JDouble(0.3))
      .withDescription("Default cpu for each operator.")

  val SQL_EXEC_DEFAULT_MEM: ConfigOption[Integer] =
    key("sql.exec.default-memory-mb")
      .defaultValue(new Integer(64))
      .withDescription("Default heap memory size for each operator.")

  val SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION: ConfigOption[JLong] =
    key("sql.exec.infer-resource.rows-per-partition")
      .defaultValue(new JLong(1000000))
      .withDescription("Sets how many rows one partition processes. " +
            "We will infer parallelism according to input row count.")

  val SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM: ConfigOption[Integer] =
    key("sql.exec.infer-resource.source.max-parallelism")
      .defaultValue(new Integer(1000))
      .withDescription("Sets max parallelism for source operator.")

  val SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION: ConfigOption[Integer] =
    key("sql.exec.infer-resource.source.mb-per-partition")
      .defaultValue(Integer.valueOf(Integer.MAX_VALUE))
      .withDescription("Sets how many data size in MB one partition processes. " +
            "We will infer the source parallelism according to source data size.")

  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM: ConfigOption[Integer] =
    key("sql.exec.infer-resource.operator.max-parallelism")
      .defaultValue(new Integer(800))
      .withDescription("Sets max parallelism for all operators.")

  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB: ConfigOption[Integer] =
    key("sql.exec.infer-resource.operator.max-memory-mb")
      .defaultValue(new Integer(1024))
      .withDescription("Maybe inferred operator mem is too large, " +
            "so this setting is upper limit for the inferred operator mem.")

  val SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU: ConfigOption[JDouble] =
    key("sql.resource.runningUnit.total-cpu")
      .defaultValue(JDouble.valueOf(0d))
      .withDescription("total cpu limit of a runningUnit. 0 means no limit.")

  // ================================ Schedule =================================

  val SQL_SCHEDULE_RUNNING_UNIT_ENABLE: ConfigOption[JBoolean] =
    key("sql.schedule.running-unit.enable")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Whether to schedule according to runningUnits.")

  // ================================= PushDown ================================

  val SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN: ConfigOption[JBoolean] =
    key("sql.exec.source.parquet.enable-predicate-pushdown")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Allow trying to push filter down to a parquet [[TableSource]]. " +
            "the default value is true, means allow the attempt.")

  val SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN: ConfigOption[JBoolean] =
    key("sql.exec.source.orc.enable-predicate-pushdown")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Allow trying to push filter down to a orc [[TableSource]]. " +
            "The default value is true, means allow the attempt.")

  // =================================== Sink ===================================
  val SQL_EXEC_SINK_MEM: ConfigOption[Integer] =
    key("sql.exec.sink.default-memory-mb")
      .defaultValue(new Integer(100))
      .withDescription("Sets the heap memory size of sink operator.")

  // TODO [[ConfigOption]] need to support no default int value
  val SQL_EXEC_SINK_PARALLELISM: ConfigOption[Integer] =
    key("sql.exec.sink.parallelism")
      .defaultValue(new Integer(-1))
      .withDescription("Sets sink parallelism if [[SQL_EXEC_SINK_PARALLELISM]] is set. If it " +
            "is not set, sink nodes will chain with ahead nodes as far as possible.")

  // =================================== Miscellaneous ===================================
  val SQL_PHYSICAL_OPERATORS_DISABLED: ConfigOption[JString] =
    key("sql.exec.operators.disabled")
      .defaultValue("")
      .withDescription("Mainly for testing. " +
            "A comma-separated list of name of the [[OperatorType]], " +
            "each name means a kind of disabled operator." +
            " Its default value is empty that means no operators are disabled. If the configure's" +
            "value is \"NestedLoopJoin, ShuffleHashJoin\", NestedLoopJoin and ShuffleHashJoin are" +
            " disabled. If the configure's value " +
            "is \"HashJoin\", ShuffleHashJoin and BroadcastHashJoin are disabled.")

  // =================================== Shuffle ===================================

  val SQL_EXEC_ALL_DATA_EXCHANGE_MODE_BATCH: ConfigOption[JBoolean] =
    key("sql.exec.all.data-exchange-mode.batch")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Sets Whether all data-exchange-mode is batch.")

  // ================================== Collect Operator Metrics ======================

  val SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED: ConfigOption[JBoolean] =
    key("sql.exec.collect.operator.metric.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("If collect operator metric is enabled, the operator metrics need to " +
            "be collected during runtime phase.")

  val SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH: ConfigOption[JString] =
    key("sql.exec.collect.operator.metric.path")
      .noDefaultValue()
      .withDescription("Sets the file path to dump stream graph plan with collected" +
            " operator metrics. Default is null.")

  // ================================== Collect Optimized Plan =======================
  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED: ConfigOption[JBoolean] =
    key("sql.cbo.collect.optimized.plan.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("If collect optimized plan is enabled, " +
            "the optimized plan need to be collected.")

  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH: ConfigOption[JString] =
    key("sql.cbo.collect.optimized.plan.path")
      .noDefaultValue()
      .withDescription("Sets the file path to dump optimized plan.")

  // =============================== Reuse Sub-Plan ===============================

  val SQL_EXEC_REUSE_SUB_PLAN_ENABLED: ConfigOption[JBoolean] =
    key("sql.exec.reuse.sub-plan.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("When true, the planner will try to find out duplicated" +
            " sub-plan and reuse them.")

  val SQL_EXEC_REUSE_TABLE_SOURCE_ENABLED: ConfigOption[JBoolean] =
    key("sql.exec.reuse.table-source.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("When true, the planner will try to find out duplicated table-source and" +
            " reuse them. This works only when `sql.exec.reuse.sub-plan.enabled` is true.")

  val SQL_EXEC_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED: ConfigOption[JBoolean] =
    key("sql.exec.reuse.nondeterministic-operator.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("When true, the planner will try to find out duplicated " +
            "nondeterministic-operator and reuse them. This works only when" +
            " [[sql.exec.reuse.sub-plan.enabled]] is true. Nondeterministic-operator contains " +
            "1. nondeterministic [[ScalarFunction]] (UDF, e.g. now)," +
            "2. nondeterministic [[AggregateFunction]](UDAF)," +
            "3. nondeterministic [[TableFunction]] (UDTF)")

  // =================================== Cbo ===================================

  val SQL_CBO_JOIN_REORDER_ENABLED: ConfigOption[JBoolean] =
    key("sql.cbo.joinReorder.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Enables join reorder in CBO. Default is disabled.")

  val SQL_CBO_AGG_PHASE_ENFORCER: ConfigOption[JString] =
    key("sql.cbo.agg.phase.enforcer")
      .defaultValue("NONE")
      .withDescription("Strategy for agg phase. Only NONE, TWO_PHASE or ONE_PHASE can be set." +
            "NONE: No special enforcer for aggregate stage. Whether to choose two stage " +
          "aggregate or one stage aggregate depends on cost." +
            "TWO_PHASE: Enforce to use two stage aggregate which has localAggregate " +
            "and globalAggregate." +
            "NOTE: 1. If aggregate call does not support split into two phase, " +
            "still use one stage aggregate." +
            "ONE_PHASE: Enforce to use one stage aggregate " +
            "which only has CompleteGlobalAggregate.")

  val SQL_CBO_SKEW_PUNISH_FACTOR: ConfigOption[Integer] =
    key("sql.cbo.skew.punish.factor")
      .defaultValue(new Integer(100))
      .withDescription("Factor to punish operator which is processing skew data.")

  val SQL_CBO_SELECTIVITY_COMPARISON_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default-comparison")
      .defaultValue(new JDouble(0.5))
      .withDescription("Sets comparison selectivity, the value should be between 0.0 (inclusive)" +
            " and 1.0 (inclusive). This value is only used for a binary comparison operator, " +
            "including <, <=, >, >=.")

  val SQL_CBO_SELECTIVITY_EQUALS_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default-equals")
      .defaultValue(new JDouble(0.15))
      .withDescription("Sets equals selectivity, the value should be between 0.0 (inclusive) and" +
            " 1.0 (inclusive). This value is only used for a binary equals operator.")

  val SQL_CBO_SELECTIVITY_ISNULL_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default-isnull")
      .defaultValue(new JDouble(0.1))
      .withDescription("Sets IS NULL selectivity, the value should be between 0.0 (inclusive) " +
            "and 1.0 (inclusive). This value is only used for IS_NULL operator.")

  val SQL_CBO_SELECTIVITY_LIKE_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default-like")
      .defaultValue(new JDouble(0.05))
      .withDescription("Sets like selectivity, the value should be between 0.0 (inclusive) and" +
            " 1.0 (inclusive). This value is only used for like operator.")

  val SQL_CBO_SELECTIVITY_AGG_CALL_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default-aggcall")
      .defaultValue(new JDouble(0.01))
      .withDescription("Sets aggCall selectivity, the value should be between 0.0 (inclusive)" +
            " and 1.0 (inclusive).")

  val SQL_CBO_SELECTIVITY_DEFAULT: ConfigOption[JDouble] =
    key("sql.cbo.selectivity.default")
      .defaultValue(new JDouble(0.25))
      .withDescription("Sets default selectivity, the value should be between 0.0 (inclusive)" +
            " and 1.0 (inclusive). This value is used for other operators.")

  val SQL_CBO_CNF_NODES_LIMIT: ConfigOption[Integer] =
    key("sql.cbo.cnf.nodes.limit")
      .defaultValue(new Integer(-1))
      .withDescription("When converting to conjunctive normal form (CNF), fail if the expression" +
            " exceeds this threshold; the threshold is expressed in terms of number of nodes " +
            "(only count RexCall node, including leaves and interior nodes). Negative number to" +
            " use the default threshold: double of number of nodes.")

  val SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED: ConfigOption[JBoolean] =
    key("sql.cbo.join.shuffle-by.partial-key.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Enables join shuffle by partial join keys. " +
            "For example: A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
            "If this flag is enabled, there are 3 shuffle strategy: 1. L and R shuffle by " +
          "c1 2. L and R shuffle by c2 3. L and R shuffle by c1 and c2")

  val SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED: ConfigOption[JBoolean] =
    key("sql.cbo.agg.shuffle-by.partial-key.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Enables aggregate shuffle by partial group keys.")

  val SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED: ConfigOption[JBoolean] =
    key("sql.cbo.rank.shuffle-by.partial-key.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Enables rank shuffle by partial partition keys.")

  // =================================== Code Gen ===================================

  val SQL_CODEGEN_MAX_LENGTH: ConfigOption[Integer] =
    key("sql.codegen.maxLength")
      .defaultValue(new Integer(48 * 1024))
      .withDescription("Generated code will be split if code length exceeds this limitation.")

  val SQL_RUNTIME_FILTER_ENABLE: ConfigOption[JBoolean] =
    key("sql.runtime-filter.enable")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Runtime filter for hash join.")

  val SQL_RUNTIME_FILTER_WAIT: ConfigOption[JBoolean] =
    key("sql.runtime-filter.wait")
      .defaultValue(JBoolean.FALSE)

  val SQL_RUNTIME_FILTER_SIZE_MAX_MB: ConfigOption[Integer] =
    key("sql.runtime-filter.size.max.mb")
      .defaultValue(Integer.valueOf(10))
      .withDescription("The max size of MB to BloomFilter. A too large BloomFilter will cause " +
            "the JobMaster bandwidth to fill up and affect scheduling.")

  val SQL_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN: ConfigOption[JDouble] =
    key("sql.runtime-filter.probe.filter-degree.min")
      .defaultValue(JDouble.valueOf(0.5))
      .withDescription("The minimum filtering degree of the probe side to enable runtime filter." +
            "(1 - buildNdv / probeNdv) * (1 - minFpp) >= minProbeFilter.")

  val SQL_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN: ConfigOption[JLong] =
    key("sql.runtime-filter.probe.row-count.min")
      .defaultValue(JLong.valueOf(100000000L))
      .withDescription("The minimum row count of probe side to enable runtime filter." +
            "Probe.rowCount >= minProbeRowCount.")

  val SQL_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX: ConfigOption[JDouble] =
    key("sql.runtime-filter.build-probe.row-count-ratio.max")
      .defaultValue(JDouble.valueOf(0.5))
      .withDescription("The rowCount of the build side and the rowCount of the probe should " +
            "have a certain ratio before using the RuntimeFilter. " +
            "Builder.rowCount / probe.rowCount <= maxRowCountRatio.")

  val SQL_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO: ConfigOption[Integer] =
    key("sql.runtime-filter.row-count.num-bits.ratio")
      .defaultValue(Integer.valueOf(40))
      .withDescription("A ratio between the probe row count and the BloomFilter size. If the " +
            "probe row count is too small, we should not use too large BloomFilter. maxBfBits = " +
            "Math.min(probeRowCount / ratioOfRowAndBits, [[SQL_RUNTIME_FILTER_SIZE_MAX_MB]])")

  val SQL_RUNTIME_FILTER_BUILDER_PUSH_DOWN_MAX_RATIO: ConfigOption[JDouble] =
    key("sql.runtime-filter.builder.push-down.max.ratio")
      .defaultValue(JDouble.valueOf(1.2))
      .withDescription("If the join key is the same, we can push down the BloomFilter" +
          " builder to the input node build, but we need to ensure that the NDV " +
          "and row count doesn't change much. " +
          "PushDownNdv / ndv <= maxRatio && pushDownRowCount / rowCount <= maxRatio.")

  val SQL_EXEC_SINK_PARQUET_BLOCK_SIZE: ConfigOption[Integer] =
    key("sql.exec.sink.parquet.block.siz")
      .defaultValue(Integer.valueOf(128 * 1024 * 1024))
      .withDescription("Parquet block size in bytes, this value would be set as " +
            "parquet.block.size and dfs.blocksize to improve alignment between " +
            "row group and hdfs block.")

  val SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE: ConfigOption[JBoolean] =
    key("sql.exec.sink.parquet.dictionary.enable")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Enable Parquet dictionary encoding.")

  val SQL_SUBSECTION_OPTIMIZATION_UNIONALL_AS_BREAKPOINT_DISABLED: ConfigOption[JBoolean] =
    key("sql.subsection-optimization.unionall-as-breakpoint.disabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Disable union all as breakpoint in subsection optimization")

  // ================================= streaming =======================================

  val SQL_EXEC_AGG_PARTIAL_BUCKET_NUM: ConfigOption[Integer] =
    key("sql.exec.partialAgg.bucket.num")
      .defaultValue(new Integer(256))
      .withDescription("Configure number of buckets in partial final mode.")

  val BLINK_MICROBATCH_ALLOW_LATENCY: ConfigOption[JLong] =
    key("blink.microBatch.allowLatencyMs")
      .defaultValue(new JLong(Long.MinValue))
      .withDescription("Microbatch allow latency (ms).")

  val BLINK_MINIBATCH_ALLOW_LATENCY: ConfigOption[JLong] =
    key("blink.miniBatch.allowLatencyMs")
      .defaultValue(new JLong(Long.MinValue))
      .withDescription("Minibatch allow latency(ms).")

  val BLINK_MINIBATCH_SIZE: ConfigOption[JLong] =
    key("blink.miniBatch.size")
      .defaultValue(new JLong(Long.MinValue))
      .withDescription("Minibatch size.")

  val SQL_EXEC_AGG_LOCAL_ENABLED: ConfigOption[JBoolean] =
    key("blink.localAgg.enabled")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Whether to enable local agg.")

  val SQL_EXEC_AGG_PARTIAL_ENABLED: ConfigOption[JBoolean] =
    key("blink.partialAgg.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Whether to enable partial agg.")

  val BLINK_MINIBATCH_JOIN_ENABLED: ConfigOption[JBoolean] =
    key("blink.miniBatch.join.enabled")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Whether to enable miniBatch join.")

  val BLINK_TOPN_APPROXIMATE_ENABLED: ConfigOption[JBoolean] =
    key("blink.topn.approximate.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Switch on/off topn approximate update rank operator.")

  val BLINK_TOPN_CACHE_SIZE: ConfigOption[JLong] =
    key("blink.topn.cache.size")
      .defaultValue(new JLong(10000L))
      .withDescription("Cache size of every topn task.")

  val BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER: ConfigOption[JLong] =
    key("blink.topn.approximate.buffer.multiplier")
      .defaultValue(new JLong(2L))
      .withDescription("In-memory sort map size multiplier (x2, for example) for topn update" +
            " rank. When approximation is enabled, default is 2. NOTE, We should make sure " +
            "sort map size limit * blink.topn.approximate.buffer.multiplier < " +
            "blink.topn.cache.size.")

  val BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE: ConfigOption[JLong] =
    key("blink.topn.approximate.buffer.minsize")
      .defaultValue(new JLong(400L))
      .withDescription("In-memory sort map size low minimal size. default is 400, " +
            "and 0 meaning no low limit for each topn job, if" +
            " buffer.multiplier * topn < buffer.minsize, then buffer is set to buffer.minsize.")

  val BLINK_VALUES_SOURCE_INPUT_ENABLED: ConfigOption[JBoolean] =
    key("blink.values.source.input.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Whether support values source input. The reason for disabling this " +
            "feature is that checkpoint will not work properly when source finished.")

  val BLINK_NON_TEMPORAL_SORT_ENABLED: ConfigOption[JBoolean] =
    key("blink.non-temporal-sort.enabled")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Switch on/off stream sort without temporal or limit." +
            "Set whether to enable universal sort for stream. When it is false, " +
            "universal sort can't use for stream, default false. Just for testing.")

  val BLINK_STATE_TTL_MS: ConfigOption[JLong] =
    key("blink.state.ttl.ms")
      .defaultValue(new JLong(JLong.MIN_VALUE))
      .withDescription("The minimum time until state that was not updated will be retained. State" +
            " might be cleared and removed if it was not updated for the defined period of time.")

  val BLINK_STATE_TTL_MAX_MS: ConfigOption[JLong] =
    key("blink.state.ttl.max.ms")
      .defaultValue(JLong.valueOf(Long.MinValue))
      .withDescription("The maximum time until state which was not updated will be retained." +
            "State will be cleared and removed if it was not updated for the defined " +
            "period of time.")

  val BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT: ConfigOption[JBoolean] =
    key("blink.miniBatch.flushBeforeSnapshot")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Whether to enable flushing buffered data before snapshot.")

  val SQL_EXEC_AGG_INCREMENTAL_ENABLED: ConfigOption[JBoolean] =
    key("blink.incrementalAgg.enabled")
      .defaultValue(JBoolean.TRUE)
      .withDescription("Whether to enable incremental aggregate.")

  val SQL_EXEC_NULL_COUNT_ADD_FILTER_MIN: ConfigOption[JLong] =
    key("sql.exec.null-count.add-filter.min")
      .defaultValue(2000000L)
}
