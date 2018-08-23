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

import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.streaming.api.graph.ShuffleProperties
import org.apache.flink.table.api.OperatorType.OperatorType
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.sources.TableSource
import org.apache.flink.util.StringUtils
import org.apache.flink.table.api.TableConfig._
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
    * Returns percentage after a max/min grouping filter of self join.
    */
  def selectivityOfSegmentTop: Double = {
    parameters.getDouble(SQL_CBO_SELECTIVITY_OF_SEGMENT_TOP,
      SQL_CBO_SELECTIVITY_OF_SEGMENT_TOP_DEFAULT)
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
  def enableCodeGenerateDebug(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
  }

  def disableCodeGenerateDebug(): Unit = {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "false")
  }

  def setCodeGenerateTmpDir(path: String): Unit = {
    if (!StringUtils.isNullOrWhitespaceOnly(path)) {
      System.setProperty("org.codehaus.janino.source_debugging.dir", path)
    } else {
      throw new RuntimeException("code generate tmp dir can't be empty")
    }
  }

  def enableBatchExternalShuffle: Boolean = {
    parameters.getBoolean(
      SQL_EXEC_EXTERNAL_SHUFFLE_ENABLED, SQL_EXEC_EXTERNAL_SHUFFLE_ENABLED_DEFAULT)
  }

  def enableRangePartition: Boolean = {
    parameters.getBoolean(
      SQL_EXEC_SORT_ENABLE_RANGE, SQL_EXEC_SORT_ENABLE_RANGE_DEFAULT)
  }

  def createShuffleProperties: ShuffleProperties = {
    val shuffleMemorySize = parameters.getInteger(
        TableConfig.SQL_EXEC_EXTERNAL_SHUFFLE_SORT_BUFFER_MEM,
        TableConfig.SQL_EXEC_EXTERNAL_SHUFFLE_SORT_BUFFER_MEM_DEFAULT)
    new ShuffleProperties(shuffleMemorySize)
  }

  /** Return max cnf node limit */
  def getMaxCnfNodeCount: Int = {
    parameters.getInteger(SQL_CBO_CNF_NODES_LIMIT, SQL_CBO_CNF_NODES_LIMIT_DEFAULT)
  }

}

object OperatorType extends Enumeration {
  type OperatorType = Value
  val NestedLoopJoin, ShuffleHashJoin, BroadcastHashJoin, SortMergeJoin, HashAgg, SortAgg = Value
}

object TableConfig {
  def DEFAULT = new TableConfig()

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

  /**
    * Sets the buffer reserved memory size for sort. It defines the lower limit for the sort.
    */
  val SQL_EXEC_SORT_BUFFER_MEM = "sql.exec.sort.buffer-memory-mb"
  /**
   * The default value for the sort's reserved and prefer buffer.
   */
  val SQL_EXEC_SORT_BUFFER_MEM_DEFAULT = 256

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

  // =================================== resource ================================

  /**
    * Sets infer resource granularity according to statics.
    * Only NONE, SOURCE or ALL can be set.
    */
  val SQL_EXEC_INFER_RESOURCE_GRANULARITY = "sql.exec.infer-resource.granularity"
  val SQL_EXEC_INFER_RESOURCE_GRANULARITY_DEFAULT = "SOURCE"

  /**
    * Sets how many rows one partition processes. We will infer the operator
    * parallelism according to input row count.
    */
  val SQL_EXEC_REL_PROCESS_ROWS_PER_PARTITION = "sql.exec.infer.rows-per-partition"
  val SQL_EXEC_REL_PROCESS_ROWS_PER_PARTITION_DEFAULT = 1000000

  /**
    * Sets max parallelism for all non-source operator.
    */
  val SQL_EXEC_INFER_REL_MAX_PARALLELISM = "sql.exec.infer.non-source.max-parallelism"
  val SQL_EXEC_INFER_REL_MAX_PARALLELISM_DEFAULT = 800

  /**
    * Sets how many data size in MB one partition processes. We will infer the
    * source parallelism according to source data size.
    */
  val SQL_EXEC_SOURCE_PROCESS_SIZE_PER_PARTITION  = "sql.exec.infer.source.mb-per-partition"
  val SQL_EXEC_SOURCE_PROCESS_SIZE_PER_PARTITION_DEFAULT: Int = 200

  /**
    * Sets max parallelism for source operator.
    */
  val SQL_EXEC_SOURCE_MAX_PARALLELISM = "sql.exec.infer.source.max-parallelism"
  val SQL_EXEC_SOURCE_MAX_PARALLELISM_DEFAULT = 1000

  /**
    * Maybe the infer's preferred manager mem is too large, so this setting is upper limit for
    * the infer's manager mem.
    */
  val SQL_EXEC_INFER_MANAGER_MAX_MEM = "sql.exec.infer.max-memory-mb"
  val SQL_EXEC_INFER_MANAGER_MAX_MEM_DEFAULT: Int = 1024

  /**
    * Maybe the infer's reserved manager mem is too small, so this setting is lower limit for
    * the infer's manager mem.
    */
  val SQL_EXEC_INFER_MANAGER_MIN_MEM = "sql.exec.infer.manager.min.memory-mb"
  val SQL_EXEC_INFER_MANAGER_MIN_MEM_DEFAULT: Int = 32

  /**
    * Whether to schedule according to runningUnits.
    */
  val SQL_EXEC_RUNNING_UNIT_SCHEDULE_ENABLE = "sql.exec.schedule.running-unit.enable"
  val SQL_EXEC_RUNNING_UNIT_SCHEDULE_ENABLE_DEFAULT = true

  /**
    * Sets Whether to adjust resource according to total cpu and memory limit of a runningUnit.
    * If not set, do not adjust resource, If set, cpu(double) and memory(in MB, long)
    * need to be provided,
    * for example: 0.5d,1024
    */
  val SQL_EXEC_ADJUST_RUNNING_UNIT_TOTAL_RESOURCE = "sql.exec.adjust.runningUnit.total-resource"

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
  /**
    * If the external shuffle is enabled, the shuffle data will go through some external storage.
    * And the shuffle data will be managed by Yarn Shuffle Service.
    */
  val SQL_EXEC_EXTERNAL_SHUFFLE_ENABLED = "sql.exec.external-shuffle.enabled"
  val SQL_EXEC_EXTERNAL_SHUFFLE_ENABLED_DEFAULT: Boolean = false

  /**
    * Sets the buffer memory size for sort at shuffle stage. It is in MB. Default is 10MB.
    */
  val SQL_EXEC_EXTERNAL_SHUFFLE_SORT_BUFFER_MEM = "sql.exec.external-shuffle.sort.buffer-memory-mb"
  val SQL_EXEC_EXTERNAL_SHUFFLE_SORT_BUFFER_MEM_DEFAULT = 10


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
    * 1. nondeterministic [[org.apache.flink.table.functions.ScalarFunction]] (UDF, e.g. now),
    * 2. nondeterministic [[org.apache.flink.table.functions.AggregateFunction]](UDAF),
    * 3. nondeterministic [[org.apache.flink.table.functions.TableFunction]] (UDTF)
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
    * Enables preference for two_phase_agg which has local agg in CBO. Default is disabled.
    */
  val SQL_CBO_AGG_TWO_PHASE_PREFERENCE_ENABLED = "sql.cbo.agg.two-phase.preference.enabled"
  val SQL_CBO_AGG_TWO_PHASE_PREFERENCE_ENABLED_DEFAULT: Boolean = false

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
  val SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED = "sql.cbo.join.shuffle.by.partial-key.enabled"
  val SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT: Boolean = false

  /**
    * Enables aggregate shuffle by partial group keys.
    */
  val SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED = "sql.cbo.agg.shuffle.by.partial-key.enabled"
  val SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED_DEFAULT: Boolean = false

  /**
    * Percentage of output records after filtered by Max/Min agg, mainly used for SegmentTop
    * operator.
    */
  val SQL_CBO_SELECTIVITY_OF_SEGMENT_TOP = "sql.cbo.selectivity.of.segment-top"
  val SQL_CBO_SELECTIVITY_OF_SEGMENT_TOP_DEFAULT: Double = 0.03D

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
   * Sets the number of per-requested buffers when the operator allocates much more segments
   * from the floating memory pool.
   */
  val SQL_EXEC_PER_REQUEST_MEM = "sql.exec.per-request.mem-mb"
  val SQL_EXEC_PER_REQUEST_MEM_DEFAULT = 32

  /**
    * Parquet block size in bytes, this value would be set as parquet.block.size and dfs.blocksize
    * to improve alignment between row group and hdfs block.
    */
  val SQL_EXEC_SINK_PARQUET_BLOCK_SIZE = "sql.exec.sink.parquet.block.size"
  val SQL_EXEC_SINK_PARQUET_BLOCK_SIZE_DEFAULT = 128 * 1024 * 1024

  /**
    * enable Parquet dictionary encoding by default.
    */
  val SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE = "sql.exec.sink.parquet.dictionary.enable"
  val SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE_DEFAULT = true
}
