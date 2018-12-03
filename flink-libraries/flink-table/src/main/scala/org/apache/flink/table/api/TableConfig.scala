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

import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigOption, ConfigOptions, Configuration, GlobalConfiguration}
import org.apache.flink.table.api.OperatorType.OperatorType
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.sources.TableSource
import org.apache.flink.util.StringUtils
import org.apache.flink.table.api.TableConfig._
import org.apache.flink.table.api.functions.{AggregateFunction, ScalarFunction, TableFunction}
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
    SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED, SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED_DEFAULT)

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
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED, SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED_DEFAULT)

  /**
    * Sets the optimizedPlanCollect.If enabled, the optimized plan need to be collected.
    */
  def setOptimizedPlanCollect(optimizedPlanCollect: Boolean): Unit = parameters.setBoolean(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED, optimizedPlanCollect)

  /**
    * Returns the file path to dump stream graph plan with collected operator metrics.
    */
  def getDumpFileOfPlanWithMetrics: String = parameters.getString(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH, SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH_DEFAULT)

  /**
    * Sets the file path to dump stream graph plan with collected operator metrics.
    */
  def setDumpFileOfPlanWithMetrics(dumpFileOfPlanWithMetrics: String): Unit = parameters.setString(
    SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH, dumpFileOfPlanWithMetrics)

  /**
    * Returns the file path to dump optimized plan.
    */
  def getDumpFileOfOptimizedPlan: String = parameters.getString(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH, SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH_DEFAULT)

  /**
    * Sets the file path to dump optimized plan.
    */
  def setDumpFileOfOptimizedPlan(dumpFileOfOptimizedPlan: String): Unit = parameters.setString(
    SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH, dumpFileOfOptimizedPlan)

  /**
    * Returns true if sub-plan reuse is enabled, else false.
    */
  def getSubPlanReuse: Boolean = parameters.getBoolean(
    SQL_EXEC_SUB_PLAN_REUSE_ENABLED, SQL_EXEC_SUB_PLAN_REUSE_ENABLED_DEFAULT)

  /**
    * Sets whether sub-plan reuse is enabled.
    */
  def setSubPlanReuse(reuseSubPlan: Boolean): Unit = parameters.setBoolean(
    SQL_EXEC_SUB_PLAN_REUSE_ENABLED, reuseSubPlan)

  /**
    * Returns true if table-source reuse is enabled, else false.
    */
  def getTableSourceReuse: Boolean = parameters.getBoolean(
    SQL_EXEC_TABLE_SOURCE_REUSE_ENABLED, SQL_EXEC_TABLE_SOURCE_REUSE_ENABLED_DEFAULT)

  /**
    * Sets whether table-source reuse is enabled.
    * This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    */
  def setTableSourceReuse(reuseSubPlan: Boolean): Unit = parameters.setBoolean(
    SQL_EXEC_TABLE_SOURCE_REUSE_ENABLED, reuseSubPlan)

  /**
    * Returns true if nondeterministic-operator reuse is enabled, else false.
    */
  def getNondeterministicOperatorReuse: Boolean = parameters.getBoolean(
    SQL_EXEC_NONDETERMINISTIC_OPERATOR_REUSE_ENABLED,
    SQL_EXEC_NONDETERMINISTIC_OPERATOR_REUSE_ENABLED_DEFAULT)

  /**
    * Sets whether nondeterministic-operator reuse is enabled.
    * This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    */
  def setNondeterministicOperatorReuse(reuseNondeterministicOperator: Boolean): Unit =
    parameters.setBoolean(SQL_EXEC_NONDETERMINISTIC_OPERATOR_REUSE_ENABLED,
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
    parameters.getBoolean(SQL_CBO_JOIN_REORDER_ENABLED, SQL_CBO_JOIN_REORDER_ENABLED_DEFAULT)
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
    parameters.getBoolean(SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED,
      SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT)
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
    parameters.getBoolean(SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED,
      SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT)
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
    parameters.getBoolean(SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED,
      SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT)
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
    * Returns whether union all is forbidden as breakpoint in subsection optimization.
    */
  def isForbidUnionAllAsBreakPointInSubsectionOptimization: Boolean =
    parameters.getBoolean(SQL_FORBID_UNIONALL_AS_BREAKPOINT_IN_SUBSECTION_OPTIMIZATION,
                          SQL_FORBID_UNIONALL_AS_BREAKPOINT_IN_SUBSECTION_OPTIMIZATION_DEFAULT)

  /**
    * Sets whether to forbid union all as breakpoint in subsection optimization. If flag is true,
    * does not create new [[org.apache.flink.table.plan.LogicalNodeBlock]] at union node even if it
    * has multiple parents, create new [[org.apache.flink.table.plan.LogicalNodeBlock]] at input
    * nodes of union node which has multiple parents.
    */
  def forbidUnionAllAsBreakPointInSubsectionOptimization(flag: Boolean): Unit = {
    parameters.setBoolean(SQL_FORBID_UNIONALL_AS_BREAKPOINT_IN_SUBSECTION_OPTIMIZATION, flag)
  }

  def enabledGivenOpType(operator: OperatorType): Boolean = {
    val disableOperators = parameters.getString(
      SQL_PHYSICAL_OPERATORS_DISABLED, SQL_PHYSICAL_OPERATORS_DISABLED_DEFAULT)
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
    parameters.getInteger(SQL_CODEGEN_MAX_LENGTH, SQL_CODEGEN_MAX_LENGTH_DEFAULT)

  /**
    * set value to 'sql.codegen.maxLength',
    * generated code will be split if code length exceeds this limitation
    */
  def setMaxGeneratedCodeLength(maxGeneratedCodeLength: Int): Unit = {
    parameters.setInteger(SQL_CODEGEN_MAX_LENGTH , maxGeneratedCodeLength)
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
    parameters.getBoolean(
      SQL_EXEC_SORT_ENABLE_RANGE, SQL_EXEC_SORT_ENABLE_RANGE_DEFAULT)
  }

  /** Return max cnf node limit */
  def getMaxCnfNodeCount: Int = {
    parameters.getInteger(SQL_CBO_CNF_NODES_LIMIT, SQL_CBO_CNF_NODES_LIMIT_DEFAULT)
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

//  /**
//    * Sets user-defined configuration
//    */
//  def setParameters(parameters: JMap[String, String]): Unit = {
//    this.parameters.addAll(parameters)
//  }

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
  /**
    * Sets whether to enable range sort, use range sort to sort all data in
    * several partitions. When it is false, sorting in only one partition, default false.
    */
  val SQL_EXEC_SORT_ENABLE_RANGE = "sql.exec.sort.enable-range"
  val SQL_EXEC_SORT_ENABLE_RANGE_DEFAULT: Boolean = false

  /**
    * Default limit when user don't set a limit after order by. This default value will be
    * invalidated if [[SQL_EXEC_SORT_ENABLE_RANGE]] is set to be true.
    */
  val SQL_EXEC_SORT_DEFAULT_LIMIT = "sql.exec.sort.default-limit"
  val SQL_EXEC_SORT_DEFAULT_LIMIT_DEFAULT: Int = 200

  val SQL_EXEC_SORT_BUFFER_MEM: ConfigOption[Integer] =
    ConfigOptions.key("sql.exec.sort.buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the buffer reserved memory size for sort. " +
          "It defines the lower limit for the sort.")

  val SQL_EXEC_SORT_PREFER_BUFFER_MEM: ConfigOption[Integer] =
    ConfigOptions.key("sql.exec.sort.prefer-buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the preferred buffer memory size for sort. " +
          "It defines the applied memory for the sort.")

  val SQL_EXEC_SORT_MAX_BUFFER_MEM: ConfigOption[Integer] =
    ConfigOptions.key("sql.exec.sort.max-buffer-memory-mb")
      .defaultValue(new Integer(256))
      .withDescription(
        "Sets the max buffer memory size for sort. " +
          "It defines the upper memory for the sort.")

  val SQL_EXEC_SORT_MAX_NUM_FILE_HANDLES: ConfigOption[Integer] =
    ConfigOptions.key("sql.exec.sort.max-num-file-handles")
        .defaultValue(new Integer(128))
        .withDescription(
          "Sort merge's maximum number of roads, too many roads, may cause too many files to be" +
              " read at the same time, resulting in excessive use of memory.")

  val SQL_EXEC_SPILL_COMPRESSION_ENABLE: ConfigOption[JBoolean] =
    ConfigOptions.key("sql.exec.spill.compression.enable")
        .defaultValue(new JBoolean(true))

  val SQL_EXEC_SPILL_COMPRESSION_CODEC: ConfigOption[String] =
    ConfigOptions.key("sql.exec.spill.compression.codec")
        .defaultValue("lz4")

  val SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE: ConfigOption[Integer] =
    ConfigOptions.key("sql.exec.spill.compression.block-size")
        .defaultValue(new Integer(64 * 1024))

  val SQL_EXEC_SORT_PARALLEL_MERGE_ENABLE: ConfigOption[JBoolean] =
    ConfigOptions.key("sql.exec.sort.parallel-merge.enable")
        .defaultValue(new JBoolean(false))

  // =================================== Join ===================================
  /**
    * Sets the HashTable reserved memory for hashJoin operator. It defines the lower limit for.
    */
  val SQL_EXEC_HASH_JOIN_TABLE_MEM = "sql.exec.hash-join.table-memory-mb"
  /**
   * The default value for the hash-join's reserved and prefer buffer.
   */
  val SQL_EXEC_HASH_JOIN_TABLE_MEM_DEFAULT = 512

  /**
    * Maximum size in bytes for data that could be broadcast to each parallel instance that holds
    * a partition of all data when performing a hash join.
    * Broadcast will be disabled if the value is -1.
    */
  val SQL_HASH_JOIN_BROADCAST_THRESHOLD = "sql.exec.hash-join.broadcast-threshold"
  val SQL_HASH_JOIN_BROADCAST_THRESHOLD_DEFAULT: Long = 1 * 1024 * 1024

  // =================================== Aggregate ===================================
  /**
    * Sets the window elements buffer limit in size used in group window agg operator.
    */
  val SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE = "sql.exec.window-agg.buffer-limit-size"
  val SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE_DEFAULT: Int = 100 * 1000

  /**
    * Sets the table reserved memory size of hashAgg operator. It defines the lower limit.
    */
  val SQL_EXEC_HASH_AGG_TABLE_MEM = "sql.exec.hash-agg.table-memory-mb"
  /**
   * The default value for the hash-agg's reserved and prefer buffer.
   */
  val SQL_EXEC_HASH_AGG_TABLE_MEM_DEFAULT = 128

  /**
    * Sets the ratio of aggregation. If outputRowCount/inputRowCount of an aggregate is less than
    * this ratio, HashAggregate would be thought better than SortAggregate.
    */
  val SQL_EXEC_AGG_GROUPS_NDV_RATIO = "sql.exec.agg.groups.ndv.ratio"
  val SQL_EXEC_AGG_GROUPS_NDV_RATIO_DEFAULT: Double = 0.8

  val SQL_EXEC_SEMI_BUILD_DISTINCT_NDV_RATIO = "sql.exec.semi.build.dictinct.ndv.ratio"
  val SQL_EXEC_SEMI_BUILD_DISTINCT_NDV_RATIO_DEFAULT: Double = 0.8

  // =================================== Buffer ===================================
  /**
    * Sets the externalBuffer memory size that is used in sortMergeJoin and overWindow.
    */
  val SQL_EXEC_EXTERNAL_BUFFER_MEM = "sql.exec.external-buffer.memory-mb"

  /**
   * The default value for the external-buffer's buffer.
   */
  val SQL_EXEC_EXTERNAL_BUFFER_MEM_DEFAULT = 10

  // =================================== Source ===================================
  /**
    * Sets the heap memory size of source operator.
    */
  val SQL_EXEC_SOURCE_MEM = "sql.exec.source.default-memory-mb"
  val SQL_EXEC_SOURCE_MEM_DEFAULT = 128

  /**
    * Sets source parallelism if [[SQL_EXEC_INFER_RESOURCE_MODE]] is NONE. If it is not set,
    * use [[SQL_EXEC_DEFAULT_PARALLELISM]] to set source parallelism.
    */
  val SQL_EXEC_SOURCE_PARALLELISM = "sql.exec.source.parallelism"

  // =================================== resource ================================

  /**
    * Sets infer resource mode according to statics.
    * Only NONE, ONLY_SOURCE or ALL can be set.
    * If set NONE, parallelism and memory of all node are set by config.
    * If set ONLY_SOURCE, only source parallelism is inferred according to statics.
    * If set ALL, parallelism and memory of all node are inferred according to statics.
    */
  val SQL_EXEC_INFER_RESOURCE_MODE= "sql.exec.infer-resource.mode"
  val SQL_EXEC_INFER_RESOURCE_MODE_DEFAULT = "NONE"

  /**
    * Default parallelism of the job. If any node do not have special
    * parallelism, use it. Its default value is the num of cpu cores in the client host.
    */
  val SQL_EXEC_DEFAULT_PARALLELISM = "sql.exec.default-parallelism"
  def SQL_EXEC_DEFAULT_PARALLELISM_DEFAULT: Int = Runtime.getRuntime.availableProcessors

  /**
    * Default cpu for each operator.

    */
  val SQL_EXEC_DEFAULT_CPU = "sql.exec.default-cpu"
  val SQL_EXEC_DEFAULT_CPU_DEFAULT = 0.3

  /**
    * Default heap memory size for each operator.
    */
  val SQL_EXEC_DEFAULT_MEM = "sql.exec.default-memory-mb"
  val SQL_EXEC_DEFAULT_MEM_DEFAULT = 64

  // infer parallelism and memory

  /**
    * Sets how many rows one partition processes. We will infer parallelism
    * according to input row count.
    */
  val SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION = "sql.exec.infer-resource.rows-per-partition"
  val SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION_DEFAULT = 1000000

  /**
    * Sets max parallelism for source operator.
    */
  val SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM =
    "sql.exec.infer-resource.source.max-parallelism"
  val SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM_DEFAULT = 1000

  /**
    * Sets how many data size in MB one partition processes. We will infer the
    * source parallelism according to source data size.
    */
  val SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION  =
    "sql.exec.infer-resource.source.mb-per-partition"
  val SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION_DEFAULT: Int = Int.MaxValue

  /**
    * Sets max parallelism for all operators.

    */
  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM =
    "sql.exec.infer-resource.operator.max-parallelism"
  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM_DEFAULT = 800

  /**
    * Maybe inferred operator mem is too large, so this setting is upper limit for
    * the inferred operator mem.
    */
  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB =
    "sql.exec.infer-resource.operator.max-memory-mb"
  val SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB_DEFAULT: Int = 1024

  val SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU: ConfigOption[JDouble] =
    ConfigOptions.key("sql.resource.runningUnit.total-cpu")
        .defaultValue(JDouble.valueOf(0d))
        .withDescription("total cpu limit of a runningUnit. 0 means no limit.")

  // ================================ Schedule =================================

  /**
    * Whether to schedule according to runningUnits.
    */
  val SQL_SCHEDULE_RUNNING_UNIT_ENABLE = "sql.schedule.running-unit.enable"
  val SQL_SCHEDULE_RUNNING_UNIT_ENABLE_DEFAULT = true

  // ================================= PushDown ================================

  /**
   * Allow trying to push filter down to a parquet [[TableSource]]. the default value is true,
   * means allow the attempt.
   */
  val SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN =
    "sql.exec.source.parquet.enable-predicate-pushdown"
  val SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN_DEFAULT = true

  /**
    * Allow trying to push filter down to a orc [[TableSource]]. the default value is true,
    * means allow the attempt.
    */
  val SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN =
    "sql.exec.source.orc.enable-predicate-pushdown"
  val SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN_DEFAULT = true

  // =================================== Sink ===================================
  /**
    * Sets the heap memory size of sink operator.
    */
  val SQL_EXEC_SINK_MEM = "sql.exec.sink.default-memory-mb"
  val SQL_EXEC_SINK_MEM_DEFAULT = 100

  /**
    * Sets sink parallelism if [[SQL_EXEC_SINK_PARALLELISM]] is set. If it is not set,
    * sink nodes will chain with ahead nodes as far as possible.
    */
  val SQL_EXEC_SINK_PARALLELISM = "sql.exec.sink.parallelism"

  // =================================== Miscellaneous ===================================
  /**
    * Mainly for testing.
    * A comma-separated list of name of the [[OperatorType]], each name means a kind of disabled
    * operator. Its default value is empty that means no operators are disabled. If the configure's
    * value is "NestedLoopJoin, ShuffleHashJoin", NestedLoopJoin and ShuffleHashJoin are disabled.
    * If the configure's value is "HashJoin", ShuffleHashJoin and BroadcastHashJoin are disabled.
    */
  val SQL_PHYSICAL_OPERATORS_DISABLED = "sql.exec.operators.disabled"
  val SQL_PHYSICAL_OPERATORS_DISABLED_DEFAULT: String = ""

  // =================================== Shuffle ===================================

  val SQL_EXEC_ALL_DATA_EXCHANGE_MODE_BATCH: ConfigOption[JBoolean] = ConfigOptions
      .key("sql.exec.all.data-exchange-mode.batch")
      .defaultValue(JBoolean.FALSE)
      .withDescription("Sets Whether all data-exchange-mode is batch.")

  // ================================== Collect Operator Metrics ======================
  /**
    * If collect operator metric is enabled, the operator metrics need to be collected
    * during runtime phase.
    */
  val SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED = "sql.exec.collect.operator.metric.enabled"
  val SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED_DEFAULT: Boolean = false

  /**
    * Sets the file path to dump stream graph plan with collected operator metrics.
    * Default is null.
    */
  val SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH = "sql.exec.collect.operator.metric.path"
  val SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH_DEFAULT: String = null

  // ================================== Collect Optimized Plan =======================
  /**
    * If collect optimized plan is enabled, the optimized plan need to be collected.
    */
  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED = "sql.cbo.collect.optimized.plan.enabled"
  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED_DEFAULT: Boolean = false

  /**
    * Sets the file path to dump optimized plan. Default is null.
    */
  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH = "sql.cbo.collect.optimized.plan.path"
  val SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH_DEFAULT: String = null

  // =============================== Reuse Sub-Plan ===============================

  /**
    * When true, the planner will try to find out duplicated sub-plan and reuse them.
    */
  val SQL_EXEC_SUB_PLAN_REUSE_ENABLED = "sql.exec.sub-plan.reuse.enabled"
  val SQL_EXEC_SUB_PLAN_REUSE_ENABLED_DEFAULT = false

  /**
    * When true, the planner will try to find out duplicated table-source and reuse them.
    * This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    */
  val SQL_EXEC_TABLE_SOURCE_REUSE_ENABLED = "sql.exec.table-source.reuse.enabled"
  val SQL_EXEC_TABLE_SOURCE_REUSE_ENABLED_DEFAULT = false

  /**
    * When true, the planner will try to find out duplicated nondeterministic-operator and
    * reuse them. This works only when `sql.exec.sub-plan.reuse.enabled` is true.
    * nondeterministic-operator contains
    * 1. nondeterministic [[ScalarFunction]] (UDF, e.g. now),
    * 2. nondeterministic [[AggregateFunction]](UDAF),
    * 3. nondeterministic [[TableFunction]] (UDTF)
    */
  val SQL_EXEC_NONDETERMINISTIC_OPERATOR_REUSE_ENABLED =
    "sql.exec.nondeterministic-operator.reuse.enabled"
  val SQL_EXEC_NONDETERMINISTIC_OPERATOR_REUSE_ENABLED_DEFAULT = false

  // =================================== Cbo ===================================

  /**
    * Enables join reorder in CBO. Default is disabled.
    */
  val SQL_CBO_JOIN_REORDER_ENABLED = "sql.cbo.joinReorder.enabled"
  val SQL_CBO_JOIN_REORDER_ENABLED_DEFAULT: Boolean = false

  /**
    * Strategy for agg phase. Only NONE, TWO_PHASE or ONE_PHASE can be set.
    * NONE: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one
    * stage aggregate depends on cost.
    * TWO_PHASE: Enforce to use two stage aggregate which has localAggregate and globalAggregate.
    * NOTE:
    * 1. If aggregate call does not support split into two phase, still use one stage aggregate.
    * ONE_PHASE: Enforce to use one stage aggregate which only has CompleteGlobalAggregate.
    * Default is NONE.
    */
  val SQL_CBO_AGG_PHASE_ENFORCER = "sql.cbo.agg.phase.enforcer"
  val SQL_CBO_AGG_PHASE_ENFORCER_DEFAULT = "NONE"

  /**
    * Factor to punish operator which is processing skew data. Default is 100.
    */
  val SQL_CBO_SKEW_PUNISH_FACTOR = "sql.cbo.skew.punish.factor"
  val SQL_CBO_SKEW_PUNISH_FACTOR_DEFAULT: Int = 100

  /**
    * Sets comparison selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    * This value is only used for a binary comparison operator, including <, <=, >, >=.
    */
  val SQL_CBO_SELECTIVITY_COMPARISON_DEFAULT = "sql.cbo.selectivity.default-comparison"

  /**
    * Sets equals selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    * This value is only used for a binary equals operator.
    */
  val SQL_CBO_SELECTIVITY_EQUALS_DEFAULT = "sql.cbo.selectivity.default-equals"

  /**
    * Sets IS NULL selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    * This value is only used for IS_NULL operator.
    */
  val SQL_CBO_SELECTIVITY_ISNULL_DEFAULT = "sql.cbo.selectivity.default-isnull"

  /**
    * Sets like selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    * This value is only used for like operator.
    */
  val SQL_CBO_SELECTIVITY_LIKE_DEFAULT = "sql.cbo.selectivity.default-like"

  /**
    * Sets aggCall selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    */
  val SQL_CBO_SELECTIVITY_AGG_CALL_DEFAULT = "sql.cbo.selectivity.default-aggcall"

  /**
    * Sets default selectivity, the value should be between 0.0 (inclusive) and 1.0 (inclusive).
    * This value is used for other operators.
    */
  val SQL_CBO_SELECTIVITY_DEFAULT = "sql.cbo.selectivity.default"

  /**
    * When converting to conjunctive normal form (CNF), fail if the expression exceeds
    * this threshold; the threshold is expressed in terms of number of nodes
    * (only count RexCall node, including leaves and interior nodes).
    * negative number to use the default threshold: double of number of nodes.
    */
  val SQL_CBO_CNF_NODES_LIMIT = "sql.cbo.cnf.nodes.limit"
  val SQL_CBO_CNF_NODES_LIMIT_DEFAULT: Int = -1

  /**
    * Enables join shuffle by partial join keys.
    * For example: A join with join condition: L.c1 = R.c1 and L.c2 = R.c2.
    * If this flag is enabled, there are 3 shuffle strategy:
    * 1. L and R shuffle by c1
    * 2. L and R shuffle by c2
    * 3. L and R shuffle by c1 and c2
    */
  val SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED = "sql.cbo.join.shuffle-by.partial-key.enabled"
  val SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT: Boolean = false

  /**
    * Enables aggregate shuffle by partial group keys.
    */
  val SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED = "sql.cbo.agg.shuffle-by.partial-key.enabled"
  val SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT: Boolean = false

  /**
    * Enables rank shuffle by partial partition keys.
    */
  val SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED = "sql.cbo.rank.shuffle-by.partial-key.enabled"
  val SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT: Boolean = false


  // =================================== Code Gen ===================================

  /**
    * generated code will be split if code length exceeds this limitation
    */
  val SQL_CODEGEN_MAX_LENGTH = "sql.codegen.maxLength"
  val SQL_CODEGEN_MAX_LENGTH_DEFAULT: Int = 48 * 1024

  /**
    * generated code will be rewrite if JVM limitations are touched
    */
  val SQL_CODEGEN_REWRITE_ENABLED = "sql.codegen.rewrite"

  /**
    * method will be split if method length exceeds
    */
  val SQL_CODEGEN_REWRITE_MAX_METHOD_LENGTH = "sql.codegen.rewrite.maxMethodLength"

  /**
    * method length max size after split
    */
  val SQL_CODEGEN_REWRITE_MAX_METHOD_LENGTH_AFTER_SPLIT =
    "sql.codegen.rewrite.maxMethodLengthAfterSplit"

  /**
    * max field number in one Java Class.
    */
  val SQL_CODEGEN_REWRITE_MAX_FIELD_COUNT = "sql.codegen.rewrite.maxFieldCount"

  /**
    * Runtime filter for hash join.
    */
  val SQL_RUNTIME_FILTER_ENABLE = "sql.runtime-filter.enable"
  val SQL_RUNTIME_FILTER_ENABLE_DEFAULT = false

  val SQL_RUNTIME_FILTER_WAIT = "sql.runtime-filter.wait"
  val SQL_RUNTIME_FILTER_WAIT_DEFAULT = false

  /**
    * The max size of MB to BloomFilter.
    * A too large BloomFilter will cause the JobMaster bandwidth to fill up and affect scheduling.
    */
  val SQL_RUNTIME_FILTER_SIZE_MAX_MB = "sql.runtime-filter.size.max.mb"
  val SQL_RUNTIME_FILTER_SIZE_MAX_MB_DEFAULT = 10

  /**
    * The minimum filtering degree of the probe side to enable runtime filter.
    * (1 - buildNdv / probeNdv) * (1 - minFpp) >= minProbeFilter.
    */
  val SQL_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN = "sql.runtime-filter.probe.filter-degree.min"
  val SQL_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN_DEFAULT = 0.5

  /**
    * The minimum row count of probe side to enable runtime filter.
    * probe.rowCount >= minProbeRowCount.
    */
  val SQL_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN = "sql.runtime-filter.probe.row-count.min"
  val SQL_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN_DEFAULT = 100000000L

  /**
    * The rowCount of the build side and the rowCount of the probe should have a certain ratio
    * before using the RuntimeFilter.
    * builder.rowCount / probe.rowCount <= maxRowCountRatio.
    */
  val SQL_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX =
    "sql.runtime-filter.build-probe.row-count-ratio.max"
  val SQL_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX_DEFAULT = 0.5

  /**
    * A ratio between the probe row count and the BloomFilter size. If the probe row count is
    * too small, we should not use too large BloomFilter.
    * maxBfBits = Math.min(probeRowCount / ratioOfRowAndBits, [[SQL_RUNTIME_FILTER_SIZE_MAX_MB]]).
    */
  val SQL_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO = "sql.runtime-filter.row-count.num-bits.ratio"
  val SQL_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO_DEFAULT = 40

  /**
    * If the join key is the same, we can push down the BloomFilter builder to the input node build,
    * but we need to ensure that the NDV and row count doesn't change much.
    * pushDownNdv / ndv <= maxRatio && pushDownRowCount / rowCount <= maxRatio.
    */
  val SQL_RUNTIME_FILTER_BUILDER_PUSH_DOWN_MAX_RATIO =
    "sql.runtime-filter.builder.push-down.max.ratio"
  val SQL_RUNTIME_FILTER_BUILDER_PUSH_DOWN_MAX_RATIO_DEFAULT = 1.2

  /**
    * Parquet block size in bytes, this value would be set as parquet.block.size and dfs.blocksize
    * to improve alignment between row group and hdfs block.
    */
  val SQL_EXEC_SINK_PARQUET_BLOCK_SIZE = "sql.exec.sink.parquet.block.size"
  val SQL_EXEC_SINK_PARQUET_BLOCK_SIZE_DEFAULT: Int = 128 * 1024 * 1024

  /**
    * enable Parquet dictionary encoding by default.
    */
  val SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE = "sql.exec.sink.parquet.dictionary.enable"
  val SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE_DEFAULT: Boolean = true

  /**
    * Forbid union all as breakpoint in subsection optimization. Default is false.
    */
  val SQL_FORBID_UNIONALL_AS_BREAKPOINT_IN_SUBSECTION_OPTIMIZATION =
    "sql.forbid.unionall.as.breakpoint.in.subsection.optimization"
  val SQL_FORBID_UNIONALL_AS_BREAKPOINT_IN_SUBSECTION_OPTIMIZATION_DEFAULT: Boolean = false

  // ================================= streaming =======================================

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

  /** whether to enable flushing buffered data before snapshot, enabled by default*/
  val BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT: ConfigOption[JBoolean] = ConfigOptions
      .key("blink.miniBatch.flushBeforeSnapshot")
      .defaultValue(true)

  /** whether to enable incremental aggregate, default is enabled/ */
  val SQL_EXEC_AGG_INCREMENTAL_ENABLED: ConfigOption[JBoolean] = ConfigOptions
      .key("blink.incrementalAgg.enabled")
      .defaultValue(true)

  val SQL_EXEC_NULL_COUNT_ADD_FILTER_MIN: ConfigOption[JLong] = ConfigOptions
      .key("sql.exec.null-count.add-filter.min")
      .defaultValue(2000000L)
}
