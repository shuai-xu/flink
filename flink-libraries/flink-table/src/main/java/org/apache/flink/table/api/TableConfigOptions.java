/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's table module.
 */
public class TableConfigOptions {

	// =================================== Sort ===================================
	public static final ConfigOption<Boolean> SQL_EXEC_SORT_ENABLE_RANGE =
			key("sql.exec.sort.enable-range")
			.defaultValue(false)
			.withDescription("Sets whether to enable range sort, use range sort to sort all data in several partitions." +
				"When it is false, sorting in only one partition");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_DEFAULT_LIMIT =
			key("sql.exec.sort.default-limit")
			.defaultValue(200)
			.withDescription("Default limit when user don't set a limit after order by. " +
				"This default value will be invalidated if [[SQL_EXEC_SORT_ENABLE_RANGE]] is set to be true.");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_BUFFER_MEM =
			key("sql.exec.sort.buffer-memory-mb")
			.defaultValue(256)
			.withDescription("Sets the buffer reserved memory size for sort. It defines the lower limit for the sort.");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_PREFER_BUFFER_MEM =
			key("sql.exec.sort.prefer-buffer-memory-mb")
			.defaultValue(256)
			.withDescription("Sets the preferred buffer memory size for sort. " +
				"It defines the applied memory for the sort.");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_MAX_BUFFER_MEM =
			key("sql.exec.sort.max-buffer-memory-mb")
			.defaultValue(256)
			.withDescription("Sets the max buffer memory size for sort. It defines the upper memory for the sort.");

	public static final ConfigOption<Integer> SQL_EXEC_SORT_MAX_NUM_FILE_HANDLES =
			key("sql.exec.sort.max-num-file-handles")
			.defaultValue(128)
			.withDescription("Sort merge's maximum number of roads, too many roads, may cause too many files to be" +
				" read at the same time, resulting in excessive use of memory.");

	public static final ConfigOption<Boolean> SQL_EXEC_SPILL_COMPRESSION_ENABLE =
			key("sql.exec.spill.compression.enable")
			.defaultValue(true);

	public static final ConfigOption<String> SQL_EXEC_SPILL_COMPRESSION_CODEC =
			key("sql.exec.spill.compression.codec")
			.defaultValue("lz4");

	public static final ConfigOption<Integer> SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE =
			key("sql.exec.spill.compression.block-size")
			.defaultValue(64 * 1024);

	public static final ConfigOption<Boolean> SQL_EXEC_SORT_ASYNC_MERGE_ENABLE =
			key("sql.exec.sort.async-merge.enable")
			.defaultValue(true);

	// =================================== Join ===================================
	public static final ConfigOption<Integer> SQL_EXEC_HASH_JOIN_TABLE_MEM =
			key("sql.exec.hash-join.table-memory-mb")
			.defaultValue(512)
			.withDescription("Sets the HashTable reserved memory for hashJoin operator. It defines the lower limit for.");

	public static final ConfigOption<Long> SQL_HASH_JOIN_BROADCAST_THRESHOLD =
			key("sql.exec.hash-join.broadcast-threshold")
			.defaultValue(1024 * 1024L)
			.withDescription("Maximum size in bytes for data that could be broadcast to each parallel instance " +
				"that holds a partition of all data when performing a hash join. " +
				"Broadcast will be disabled if the value is -1.");

	// =================================== Aggregate ===================================

	public static final ConfigOption<Integer> SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE =
			key("sql.exec.window-agg.buffer-limit-size")
			.defaultValue(100 * 1000)
			.withDescription("Sets the window elements buffer limit in size used in group window agg operator.");

	public static final ConfigOption<Integer> SQL_EXEC_HASH_AGG_TABLE_MEM =
			key("sql.exec.hash-agg.table-memory-mb")
			.defaultValue(128)
			.withDescription("Sets the table reserved memory size of hashAgg operator. It defines the lower limit.");

	public static final ConfigOption<Double> SQL_EXEC_AGG_GROUPS_NDV_RATIO =
			key("sql.exec.agg.groups.ndv.ratio")
			.defaultValue(0.8)
			.withDescription("Sets the ratio of aggregation. If outputRowCount/inputRowCount of an aggregate is less " +
				"than this ratio, HashAggregate would be thought better than SortAggregate.");

	public static final ConfigOption<Double> SQL_EXEC_SEMI_BUILD_DISTINCT_NDV_RATIO =
			key("sql.exec.semi.build.dictinct.ndv.ratio")
			.defaultValue(0.8);

	// =================================== Buffer ===================================

	public static final ConfigOption<Integer> SQL_EXEC_EXTERNAL_BUFFER_MEM =
			key("sql.exec.external-buffer.memory-mb")
			.defaultValue(128)
			.withDescription("Sets the externalBuffer memory size that is used in sortMergeJoin and overWindow.");

	// =================================== Source ===================================
	public static final ConfigOption<Integer> SQL_EXEC_SOURCE_MEM =
			key("sql.exec.source.default-memory-mb")
			.defaultValue(128)
			.withDescription("Sets the heap memory size of source operator.");

	// TODO [[ConfigOption]] need to support no default int value
	public static final ConfigOption<Integer> SQL_EXEC_SOURCE_PARALLELISM =
			key("sql.exec.source.parallelism")
			.defaultValue(-1)
			.withDescription("Sets source parallelism if [[SQL_EXEC_INFER_RESOURCE_MODE]] is NONE." +
				"If it is not set, use [[SQL_EXEC_DEFAULT_PARALLELISM]] to set source parallelism.");

	// =================================== resource ================================

	public static final ConfigOption<String> SQL_EXEC_INFER_RESOURCE_MODE =
			key("sql.exec.infer-resource.mode")
			.defaultValue("NONE")
			.withDescription("Sets infer resource mode according to statics. Only NONE, ONLY_SOURCE or ALL can be set." +
				"If set NONE, parallelism and memory of all node are set by config." +
				"If set ONLY_SOURCE, only source parallelism is inferred according to statics." +
				"If set ALL, parallelism and memory of all node are inferred according to statics.");

	// TODO [[ConfigOption]] need to support no default int value
	public static final ConfigOption<Integer> SQL_EXEC_DEFAULT_PARALLELISM =
			key("sql.exec.default-parallelism")
			.defaultValue(-1)
			.withDescription("Default parallelism of the job. If any node do not have special parallelism, use it." +
				"Its default value is the num of cpu cores in the client host.");

	public static final ConfigOption<Double> SQL_EXEC_DEFAULT_CPU =
			key("sql.exec.default-cpu")
			.defaultValue(0.3)
			.withDescription("Default cpu for each operator.");

	public static final ConfigOption<Integer> SQL_EXEC_DEFAULT_MEM =
			key("sql.exec.default-memory-mb")
			.defaultValue(64)
			.withDescription("Default heap memory size for each operator.");

	public static final ConfigOption<Long> SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION =
			key("sql.exec.infer-resource.rows-per-partition")
			.defaultValue(1000000L)
			.withDescription("Sets how many rows one partition processes. We will infer parallelism according " +
				"to input row count.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM =
			key("sql.exec.infer-resource.source.max-parallelism")
			.defaultValue(1000)
			.withDescription("Sets max parallelism for source operator.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION =
			key("sql.exec.infer-resource.source.mb-per-partition")
			.defaultValue(Integer.MAX_VALUE)
			.withDescription("Sets how many data size in MB one partition processes. We will infer the source parallelism" +
				" according to source data size.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM =
			key("sql.exec.infer-resource.operator.max-parallelism")
			.defaultValue(800)
			.withDescription("Sets max parallelism for all operators.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB =
			key("sql.exec.infer-resource.operator.max-memory-mb")
			.defaultValue(1024)
			.withDescription("Maybe inferred operator mem is too large, so this setting is upper limit for the" +
				" inferred operator mem.");

	public static final ConfigOption<Double> SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU =
			key("sql.resource.runningUnit.total-cpu")
			.defaultValue(0d)
			.withDescription("total cpu limit of a runningUnit. 0 means no limit.");

	// ================================ Schedule =================================

	public static final ConfigOption<Boolean> SQL_SCHEDULE_RUNNING_UNIT_ENABLE =
			key("sql.schedule.running-unit.enable")
			.defaultValue(true)
			.withDescription("Whether to schedule according to runningUnits.");

	// ================================= PushDown ================================

	public static final ConfigOption<Boolean> SQL_EXEC_SOURCE_PARQUET_ENABLE_PREDICATE_PUSHDOWN =
			key("sql.exec.source.parquet.enable-predicate-pushdown")
			.defaultValue(true)
			.withDescription("Allow trying to push filter down to a parquet [[TableSource]]. the default value is true, " +
				"means allow the attempt.");

	public static final ConfigOption<Boolean> SQL_EXEC_SOURCE_ORC_ENABLE_PREDICATE_PUSHDOWN =
			key("sql.exec.source.orc.enable-predicate-pushdown")
			.defaultValue(true)
			.withDescription("Allow trying to push filter down to a orc [[TableSource]]. The default value is true, " +
				"means allow the attempt.");

	// =================================== Sink ===================================
	public static final ConfigOption<Integer> SQL_EXEC_SINK_MEM =
			key("sql.exec.sink.default-memory-mb")
			.defaultValue(100)
			.withDescription("Sets the heap memory size of sink operator.");

	// TODO [[ConfigOption]] need to support no default int value
	public static final ConfigOption<Integer> SQL_EXEC_SINK_PARALLELISM =
			key("sql.exec.sink.parallelism")
			.defaultValue(-1)
			.withDescription("Sets sink parallelism if [[SQL_EXEC_SINK_PARALLELISM]] is set. If it is not set, " +
					"sink nodes will chain with ahead nodes as far as possible.");

	// =================================== Miscellaneous ===================================
	public static final ConfigOption<String> SQL_PHYSICAL_OPERATORS_DISABLED =
			key("sql.exec.operators.disabled")
			.defaultValue("")
			.withDescription("Mainly for testing. A comma-separated list of name of the [[OperatorType]], each name " +
				"means a kind of disabled operator. Its default value is empty that means no operators are disabled. " +
				"If the configure's value is \"NestedLoopJoin, ShuffleHashJoin\", NestedLoopJoin and ShuffleHashJoin " +
				"are disabled. If the configure's value is \"HashJoin\", " +
				"ShuffleHashJoin and BroadcastHashJoin are disabled.");

	// =================================== Shuffle ===================================

	public static final ConfigOption<Boolean> SQL_EXEC_ALL_DATA_EXCHANGE_MODE_BATCH =
			key("sql.exec.all.data-exchange-mode.batch")
			.defaultValue(false)
			.withDescription("Sets Whether all data-exchange-mode is batch.");

	// ================================== Collect Operator Metrics ======================

	public static final ConfigOption<Boolean> SQL_EXEC_COLLECT_OPERATOR_METRIC_ENABLED =
			key("sql.exec.collect.operator.metric.enabled")
			.defaultValue(false)
			.withDescription("If collect operator metric is enabled, the operator metrics need to " +
							"be collected during runtime phase.");

	public static final ConfigOption<String> SQL_EXEC_COLLECT_OPERATOR_METRIC_PATH =
			key("sql.exec.collect.operator.metric.path")
			.noDefaultValue()
			.withDescription("Sets the file path to dump stream graph plan with collected operator metrics." +
				" Default is null.");

	// ================================== Collect Optimized Plan =======================
	public static final ConfigOption<Boolean> SQL_CBO_COLLECT_OPTIMIZED_PLAN_ENABLED =
			key("sql.cbo.collect.optimized.plan.enabled")
			.defaultValue(false)
			.withDescription("If collect optimized plan is enabled, the optimized plan need to be collected.");

	public static final ConfigOption<String> SQL_CBO_COLLECT_OPTIMIZED_PLAN_PATH =
			key("sql.cbo.collect.optimized.plan.path")
			.noDefaultValue()
			.withDescription("Sets the file path to dump optimized plan.");

	// =============================== Reuse Sub-Plan ===============================

	public static final ConfigOption<Boolean> SQL_EXEC_REUSE_SUB_PLAN_ENABLED =
			key("sql.exec.reuse.sub-plan.enabled")
			.defaultValue(false)
			.withDescription("When true, the planner will try to find out duplicated" +
							" sub-plan and reuse them.");

	public static final ConfigOption<Boolean> SQL_EXEC_REUSE_TABLE_SOURCE_ENABLED =
			key("sql.exec.reuse.table-source.enabled")
			.defaultValue(false)
			.withDescription("When true, the planner will try to find out duplicated table-source and reuse them. " +
				"This works only when `sql.exec.reuse.sub-plan.enabled` is true.");

	public static final ConfigOption<Boolean> SQL_EXEC_REUSE_NONDETERMINISTIC_OPERATOR_ENABLED =
			key("sql.exec.reuse.nondeterministic-operator.enabled")
			.defaultValue(false)
			.withDescription("When true, the planner will try to find out duplicated nondeterministic-operator and" +
				" reuse them. This works only when [[sql.exec.reuse.sub-plan.enabled]] is true. " +
				"Nondeterministic-operator contains 1. nondeterministic [[ScalarFunction]] (UDF, e.g. now)," +
				"2. nondeterministic [[AggregateFunction]](UDAF), 3. nondeterministic [[TableFunction]] (UDTF)");

	// =================================== Cbo ===================================

	public static final ConfigOption<Boolean> SQL_CBO_JOIN_REORDER_ENABLED =
			key("sql.cbo.joinReorder.enabled")
			.defaultValue(false)
			.withDescription("Enables join reorder in CBO. Default is disabled.");

	public static final ConfigOption<String> SQL_CBO_AGG_PHASE_ENFORCER =
			key("sql.cbo.agg.phase.enforcer")
			.defaultValue("NONE")
			.withDescription("Strategy for agg phase. Only NONE, TWO_PHASE or ONE_PHASE can be set." +
				"NONE: No special enforcer for aggregate stage. Whether to choose two stage aggregate or one" +
				" stage aggregate depends on cost. TWO_PHASE: Enforce to use two stage aggregate which has " +
				"localAggregate and globalAggregate.NOTE: 1. If aggregate call does not support split into" +
				" two phase, still use one stage aggregate. ONE_PHASE: Enforce to use one stage aggregate " +
				"which only has CompleteGlobalAggregate.");

	public static final ConfigOption<Integer> SQL_CBO_SKEW_PUNISH_FACTOR =
			key("sql.cbo.skew.punish.factor")
			.defaultValue(100)
			.withDescription("Factor to punish operator which is processing skew data.");

	public static final ConfigOption<Integer> SQL_CBO_CNF_NODES_LIMIT =
			key("sql.cbo.cnf.nodes.limit")
			.defaultValue(-1)
			.withDescription("When converting to conjunctive normal form (CNF), fail if the expression" +
				" exceeds this threshold; the threshold is expressed in terms of number of nodes " +
				"(only count RexCall node, including leaves and interior nodes). Negative number to" +
				" use the default threshold: double of number of nodes.");

	public static final ConfigOption<Boolean> SQL_CBO_JOIN_SHUFFLE_BY_PARTIALKEY_ENABLED =
			key("sql.cbo.join.shuffle-by.partial-key.enabled")
			.defaultValue(false)
			.withDescription("Enables join shuffle by partial join keys. " +
				"For example: A join with join condition: L.c1 = R.c1 and L.c2 = R.c2. " +
				"If this flag is enabled, there are 3 shuffle strategy: 1. L and R shuffle by " +
				"c1 2. L and R shuffle by c2 3. L and R shuffle by c1 and c2");

	public static final ConfigOption<Boolean> SQL_CBO_AGG_SHUFFLE_BY_PARTIALKEY_ENABLED =
			key("sql.cbo.agg.shuffle-by.partial-key.enabled")
			.defaultValue(false)
			.withDescription("Enables aggregate shuffle by partial group keys.");

	public static final ConfigOption<Boolean> SQL_CBO_RANK_SHUFFLE_BY_PARTIALKEY_ENABLED =
			key("sql.cbo.rank.shuffle-by.partial-key.enabled")
			.defaultValue(false)
			.withDescription("Enables rank shuffle by partial partition keys.");

	// =================================== Code Gen ===================================

	public static final ConfigOption<Integer> SQL_CODEGEN_MAX_LENGTH =
			key("sql.codegen.maxLength")
			.defaultValue(48 * 1024)
			.withDescription("Generated code will be split if code length exceeds this limitation.");

	public static final ConfigOption<Boolean> SQL_RUNTIME_FILTER_ENABLE =
			key("sql.runtime-filter.enable")
			.defaultValue(false)
			.withDescription("Runtime filter for hash join.");

	public static final ConfigOption<Boolean> SQL_RUNTIME_FILTER_WAIT =
			key("sql.runtime-filter.wait")
			.defaultValue(false);

	public static final ConfigOption<Integer> SQL_RUNTIME_FILTER_SIZE_MAX_MB =
			key("sql.runtime-filter.size.max.mb")
			.defaultValue(10)
			.withDescription("The max size of MB to BloomFilter. A too large BloomFilter will cause " +
				"the JobMaster bandwidth to fill up and affect scheduling.");

	public static final ConfigOption<Double> SQL_RUNTIME_FILTER_PROBE_FILTER_DEGREE_MIN =
			key("sql.runtime-filter.probe.filter-degree.min")
			.defaultValue(0.5)
			.withDescription("The minimum filtering degree of the probe side to enable runtime filter." +
				"(1 - buildNdv / probeNdv) * (1 - minFpp) >= minProbeFilter.");

	public static final ConfigOption<Long> SQL_RUNTIME_FILTER_PROBE_ROW_COUNT_MIN =
			key("sql.runtime-filter.probe.row-count.min")
			.defaultValue(100000000L)
			.withDescription("The minimum row count of probe side to enable runtime filter." +
				"Probe.rowCount >= minProbeRowCount.");

	public static final ConfigOption<Double> SQL_RUNTIME_FILTER_BUILD_PROBE_ROW_COUNT_RATIO_MAX =
			key("sql.runtime-filter.build-probe.row-count-ratio.max")
			.defaultValue(0.5)
			.withDescription("The rowCount of the build side and the rowCount of the probe should " +
				"have a certain ratio before using the RuntimeFilter. " +
				"Builder.rowCount / probe.rowCount <= maxRowCountRatio.");

	public static final ConfigOption<Integer> SQL_RUNTIME_FILTER_ROW_COUNT_NUM_BITS_RATIO =
			key("sql.runtime-filter.row-count.num-bits.ratio")
			.defaultValue(40)
			.withDescription("A ratio between the probe row count and the BloomFilter size. If the " +
				"probe row count is too small, we should not use too large BloomFilter. maxBfBits = " +
				"Math.min(probeRowCount / ratioOfRowAndBits, [[SQL_RUNTIME_FILTER_SIZE_MAX_MB]])");

	public static final ConfigOption<Double> SQL_RUNTIME_FILTER_BUILDER_PUSH_DOWN_MAX_RATIO =
			key("sql.runtime-filter.builder.push-down.max.ratio")
			.defaultValue(1.2)
			.withDescription("If the join key is the same, we can push down the BloomFilter" +
				" builder to the input node build, but we need to ensure that the NDV " +
				"and row count doesn't change much. " +
				"PushDownNdv / ndv <= maxRatio && pushDownRowCount / rowCount <= maxRatio.");

	public static final ConfigOption<Integer> SQL_EXEC_SINK_PARQUET_BLOCK_SIZE =
			key("sql.exec.sink.parquet.block.siz")
			.defaultValue(128 * 1024 * 1024)
			.withDescription("Parquet block size in bytes, this value would be set as " +
				"parquet.block.size and dfs.blocksize to improve alignment between " +
				"row group and hdfs block.");

	public static final ConfigOption<Boolean> SQL_EXEC_SINK_PARQUET_DICTIONARY_ENABLE =
			key("sql.exec.sink.parquet.dictionary.enable")
			.defaultValue(true)
			.withDescription("Enable Parquet dictionary encoding.");

	public static final ConfigOption<Boolean> SQL_SUBSECTION_OPTIMIZATION_UNIONALL_AS_BREAKPOINT_DISABLED =
			key("sql.subsection-optimization.unionall-as-breakpoint.disabled")
			.defaultValue(false)
			.withDescription("Disable union all as breakpoint in subsection optimization");

	// ================================= streaming =======================================

	public static final ConfigOption<Integer> SQL_EXEC_AGG_PARTIAL_BUCKET_NUM =
			key("sql.exec.partialAgg.bucket.num")
			.defaultValue(256)
			.withDescription("Configure number of buckets in partial final mode.");

	public static final ConfigOption<Long> BLINK_MICROBATCH_ALLOW_LATENCY =
			key("blink.microBatch.allowLatencyMs")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("Microbatch allow latency (ms).");

	public static final ConfigOption<Long> BLINK_MINIBATCH_ALLOW_LATENCY =
			key("blink.miniBatch.allowLatencyMs")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("Minibatch allow latency(ms).");

	public static final ConfigOption<Long> BLINK_MINIBATCH_SIZE =
			key("blink.miniBatch.size")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("Minibatch size.");

	public static final ConfigOption<Boolean> SQL_EXEC_AGG_LOCAL_ENABLED =
			key("blink.localAgg.enabled")
			.defaultValue(true)
			.withDescription("Whether to enable local agg.");

	public static final ConfigOption<Boolean> SQL_EXEC_AGG_PARTIAL_ENABLED =
			key("blink.partialAgg.enabled")
			.defaultValue(false)
			.withDescription("Whether to enable partial agg.");

	public static final ConfigOption<Boolean> BLINK_MINIBATCH_JOIN_ENABLED =
			key("blink.miniBatch.join.enabled")
			.defaultValue(true)
			.withDescription("Whether to enable miniBatch join.");

	public static final ConfigOption<Boolean> BLINK_TOPN_APPROXIMATE_ENABLED =
			key("blink.topn.approximate.enabled")
			.defaultValue(false)
			.withDescription("Switch on/off topn approximate update rank operator.");

	public static final ConfigOption<Long> BLINK_TOPN_CACHE_SIZE =
			key("blink.topn.cache.size")
			.defaultValue(10000L)
			.withDescription("Cache size of every topn task.");

	public static final ConfigOption<Long> BLINK_TOPN_APPROXIMATE_BUFFER_MULTIPLIER =
			key("blink.topn.approximate.buffer.multiplier")
			.defaultValue(2L)
			.withDescription("In-memory sort map size multiplier (x2, for example) for topn update" +
				" rank. When approximation is enabled, default is 2. NOTE, We should make sure " +
				"sort map size limit * blink.topn.approximate.buffer.multiplier < " +
				"blink.topn.cache.size.");

	public static final ConfigOption<Long> BLINK_TOPN_APPROXIMATE_BUFFER_MINSIZE =
			key("blink.topn.approximate.buffer.minsize")
			.defaultValue(400L)
			.withDescription("In-memory sort map size low minimal size. default is 400, " +
				"and 0 meaning no low limit for each topn job, if" +
				" buffer.multiplier * topn < buffer.minsize, then buffer is set to buffer.minsize.");

	public static final ConfigOption<Boolean> BLINK_VALUES_SOURCE_INPUT_ENABLED =
			key("blink.values.source.input.enabled")
			.defaultValue(false)
			.withDescription("Whether support values source input. The reason for disabling this " +
				"feature is that checkpoint will not work properly when source finished.");

	public static final ConfigOption<Boolean> BLINK_NON_TEMPORAL_SORT_ENABLED =
			key("blink.non-temporal-sort.enabled")
			.defaultValue(false)
			.withDescription("Switch on/off stream sort without temporal or limit." +
				"Set whether to enable universal sort for stream. When it is false, " +
				"universal sort can't use for stream, default false. Just for testing.");

	public static final ConfigOption<Long> BLINK_STATE_TTL_MS =
			key("blink.state.ttl.ms")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("The minimum time until state that was not updated will be retained. State" +
				" might be cleared and removed if it was not updated for the defined period of time.");

	public static final ConfigOption<Long> BLINK_STATE_TTL_MAX_MS =
			key("blink.state.ttl.max.ms")
			.defaultValue(Long.MIN_VALUE)
			.withDescription("The maximum time until state which was not updated will be retained." +
				"State will be cleared and removed if it was not updated for the defined " +
				"period of time.");

	public static final ConfigOption<Boolean> BLINK_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT =
			key("blink.miniBatch.flushBeforeSnapshot")
			.defaultValue(true)
			.withDescription("Whether to enable flushing buffered data before snapshot.");

	public static final ConfigOption<Boolean> SQL_EXEC_AGG_INCREMENTAL_ENABLED =
			key("blink.incrementalAgg.enabled")
			.defaultValue(true)
			.withDescription("Whether to enable incremental aggregate.");

	public static final ConfigOption<Long> SQL_EXEC_NULL_COUNT_ADD_FILTER_MIN =
			key("sql.exec.null-count.add-filter.min")
			.defaultValue(2000000L);
}
