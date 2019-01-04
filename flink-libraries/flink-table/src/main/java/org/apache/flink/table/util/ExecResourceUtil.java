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

package org.apache.flink.table.util;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Deal with resource config.
 */
public class ExecResourceUtil {

	/**
	 * How many Bytes per MB.
	 */
	public static final long SIZE_IN_MB =  1024L * 1024;

	/**
	 * Sets the HashTable preferred memory for hashJoin operator. It defines the upper limit.
	 */
	public static final ConfigOption<Integer> SQL_EXEC_HASH_JOIN_TABLE_PREFER_MEM =
			key("sql.exec.hash-join.table-prefer-memory-mb")
			.defaultValue(-1)
			.withDescription("Sets the HashTable preferred memory for hashJoin operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_EXEC_HASH_AGG_TABLE_PREFER_MEM =
			key("sql.exec.hash-agg.table-prefer-memory-mb")
			.defaultValue(-1)
			.withDescription("Sets the table preferred memory size of hashAgg operator. It defines the upper limit.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM =
			key("sql.exec.infer-resource.operator.min-parallelism")
			.defaultValue(1)
			.withDescription("Sets min parallelism for operators.");

	public static final ConfigOption<Integer> SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB =
			key("sql.exec.infer-resource.operator.min-memory-mb")
			.defaultValue(32)
			.withDescription("Maybe the infer's reserved manager mem is too small, so this " +
					"setting is lower limit for the infer's manager mem.");

	public static final ConfigOption<Double> SQL_EXEC_INFER_RESERVED_MEM_DISCOUNT =
			key("sql.exec.infer-resource.reserve-mem.discount")
			.defaultValue(1.0d)
			.withDescription("Sets reserve mem discount.");

	public static final ConfigOption<Integer> SQL_EXEC_PER_REQUEST_MEM =
			key("sql.exec.per-request.mem-mb")
			.defaultValue(32)
			.withDescription("Sets the number of per-requested buffers when the operator " +
					"allocates much more segments from the floating memory pool.");

	public static double getDefaultCpu(Configuration tableConf) {
		return tableConf.getDouble(
				TableConfigOptions.SQL_EXEC_DEFAULT_CPU);
	}

	/**
	 * Gets default parallelism of operator.
	 * @param tableConf Configuration.
	 * @return default parallelism of operator.
	 */
	public static int getOperatorDefaultParallelism(Configuration tableConf, int envParallelism) {
		int parallelism = tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_DEFAULT_PARALLELISM);
		if (parallelism <= 0) {
			parallelism = envParallelism;
		}
		return parallelism;
	}

	/**
	 * Gets default resourceSpec for a operator that has no specific resource need.
	 * For converter rel.
	 * @param tableConf Configuration.
	 * @return default resourceSpec for a operator that has no specific resource need.
	 */
	public static ResourceSpec getDefaultResourceSpec(Configuration tableConf) {
		ResourceSpec.Builder builder = new ResourceSpec.Builder();
		builder.setCpuCores(getDefaultCpu(tableConf));
		builder.setHeapMemoryInMB(getDefaultHeapMem(tableConf));
		return builder.build();
	}

	/**
	 * Gets resourceSpec which specific heapMemory size. For sink rel.
	 * @param tableConf Configuration.
	 * @param heapMemory the specific heapMemory size.
	 * @return resourceSpec which specific heapMemory size.
	 */
	public static ResourceSpec getResourceSpec(
			Configuration tableConf,
			int heapMemory) {
		ResourceSpec.Builder builder = new ResourceSpec.Builder();
		builder.setCpuCores(getDefaultCpu(tableConf));
		builder.setHeapMemoryInMB(heapMemory);
		return builder.build();
	}

	public static int getDefaultHeapMem(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_DEFAULT_MEM);
	}

	/**
	 * Gets the config managedMemory for sort buffer.
	 * @param tableConf Configuration.
	 * @return the config managedMemory for sort buffer.
	 */
	public static int getSortBufferManagedMemory(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SORT_BUFFER_MEM);
	}

	/**
	 * Gets the preferred managedMemory for sort buffer.
	 * @param tableConf Configuration.
	 * @return the prefer managedMemory for sort buffer.
	 */
	public static int getSortBufferManagedPreferredMemory(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SORT_PREFER_BUFFER_MEM);
	}

	/**
	 * Gets the max managedMemory for sort buffer.
	 * @param tableConf Configuration.
	 * @return the max managedMemory for sort buffer.
	 */
	public static int getSortBufferManagedMaxMemory(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SORT_MAX_BUFFER_MEM);
	}

	/**
	 * Gets the config managedMemory for external buffer.
	 * @param tableConf Configuration.
	 * @return the config managedMemory for external buffer.
	 */
	public static int getExternalBufferManagedMemory(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_EXTERNAL_BUFFER_MEM);
	}

	/**
	 * Gets the config managedMemory for hashJoin table.
	 * @param tableConf Configuration.
	 * @return the config managedMemory for hashJoin table.
	 */
	public static int getHashJoinTableManagedMemory(Configuration tableConf) {
		return tableConf.getInteger(TableConfigOptions.SQL_EXEC_HASH_JOIN_TABLE_MEM);
	}

	/**
	 * Gets the config memory for source.
	 * @param tableConf Configuration.
	 * @return the config memory for source.
	 */
	public static int getSourceMem(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SOURCE_MEM);
	}

	/**
	 * Gets the config parallelism for source.
	 * @param tableConf Configuration.
	 * @return the config parallelism for source.
	 */
	public static int getSourceParallelism(Configuration tableConf, int envParallelism) {
		int parallelism = tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SOURCE_PARALLELISM);
		if (parallelism <= 0) {
			parallelism = getOperatorDefaultParallelism(tableConf, envParallelism);
		}
		return parallelism;
	}

	/**
	 * Gets the config parallelism for sink. If it is not set, return -1.
	 * @param tableConf Configuration.
	 * @return the config parallelism for sink.
	 */
	public static int getSinkParallelism(Configuration tableConf) {
		return tableConf.getInteger(TableConfigOptions.SQL_EXEC_SINK_PARALLELISM);
	}

	/**
	 * Gets the config memory for sink.
	 * @param tableConf Configuration.
	 * @return the config memory for sink.
	 */
	public static int getSinkMem(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_SINK_MEM);
	}

	/**
	 * Gets the config managedMemory for hashAgg.
	 * @param tableConf Configuration.
	 * @return the config managedMemory for hashAgg.
	 */
	public static int getHashAggManagedMemory(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_HASH_AGG_TABLE_MEM);
	}

	/**
	 * Gets the config managedMemory for hashAgg.
	 * @param tableConf Configuration.
	 * @return the config managedMemory for hashAgg.
	 */
	public static int getWindowAggBufferLimitSize(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE);
	}

	/**
	 * Gets the config row count that one partition processes.
	 * @param tableConf Configuration.
	 * @return the config row count that one partition processes.
	 */
	public static long getRelCountPerPartition(Configuration tableConf) {
		return tableConf.getLong(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION);
	}

	/**
	 * Gets the config data size that one partition processes.
	 * @param tableConf Configuration.
	 * @return the config data size that one partition processes.
	 */
	public static int getSourceSizePerPartition(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION);
	}

	/**
	 * Gets the config max num of source parallelism.
	 * @param tableConf Configuration.
	 * @return the config max num of source parallelism.
	 */
	public static int getSourceMaxParallelism(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM);
	}

	/**
	 * Gets the config max num of operator parallelism.
	 * @param tableConf Configuration.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMaxParallelism(Configuration tableConf) {
		return tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM);
	}

	/**
	 * Gets the config min num of operator parallelism.
	 * @param tableConf Configuration.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMinParallelism(Configuration tableConf) {
		return tableConf.getInteger(SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM);
	}

	/**
	 * Calculates operator parallelism based on rowcount of the operator.
	 * @param rowCount rowCount of the operator
	 * @param tableConf Configuration.
	 * @return the result of operator parallelism.
	 */
	public static int calOperatorParallelism(double rowCount, Configuration tableConf) {
		int maxParallelism = getOperatorMaxParallelism(tableConf);
		int minParallelism = getOperatorMinParallelism(tableConf);
		int resultParallelism = (int) (rowCount / getRelCountPerPartition(tableConf));
		return Math.max(Math.min(resultParallelism, maxParallelism), minParallelism);
	}

	/**
	 * Gets the preferred managedMemory for hashJoin table.
	 * @param tableConf Configuration.
	 * @return the preferred managedMemory for hashJoin table.
	 */
	public static int getHashJoinTableManagedPreferredMemory(Configuration tableConf) {
		int memory = tableConf.getInteger(SQL_EXEC_HASH_JOIN_TABLE_PREFER_MEM);
		if (memory <= 0) {
			memory = getHashJoinTableManagedMemory(tableConf);
		}
		return memory;
	}

	/**
	 * Gets the preferred managedMemory for hashAgg.
	 * @param tableConf Configuration.
	 * @return the preferred managedMemory for hashAgg.
	 */
	public static int getHashAggManagedPreferredMemory(Configuration tableConf) {
		int memory = tableConf.getInteger(SQL_EXEC_HASH_AGG_TABLE_PREFER_MEM);
		if (memory <= 0) {
			memory = getHashAggManagedMemory(tableConf);
		}
		return memory;
	}

	/**
	 * Gets the min managedMemory.
	 * @param tableConf Configuration.
	 * @return the min managedMemory.
	 */
	public static int getOperatorMinManagedMem(Configuration tableConf) {
		return tableConf.getInteger(SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB);
	}

	/**
	 * get the reserved, preferred and max managed mem by the inferred mem. And they should be between the maximum
	 * and the minimum mem.
	 *
	 * @param tableConf Configuration.
	 * @param memCostInMB the infer mem of per partition.
	 */
	public static Tuple3<Integer, Integer, Integer> reviseAndGetInferManagedMem(Configuration tableConf, int memCostInMB) {
		double reservedDiscount = tableConf.getDouble(SQL_EXEC_INFER_RESERVED_MEM_DISCOUNT);
		if (reservedDiscount > 1 || reservedDiscount <= 0) {
			throw new IllegalArgumentException(SQL_EXEC_INFER_RESERVED_MEM_DISCOUNT + " should be > 0 and <= 1");
		}

		int maxMem = tableConf.getInteger(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB);

		int minMem = getOperatorMinManagedMem(tableConf);

		int preferMem = Math.max(Math.min(maxMem, memCostInMB), minMem);

		int reservedMem = Math.max((int) (preferMem * reservedDiscount), minMem);

		return new Tuple3<>(reservedMem, preferMem, maxMem);
	}

	/**
	 * Gets the managedMemory for per-allocating.
	 */
	public static int getPerRequestManagedMemory(Configuration tableConf) {
		return tableConf.getInteger(SQL_EXEC_PER_REQUEST_MEM);
	}

	/**
	 * Whether to enable schedule with runningUnit.
	 */
	public static boolean enableRunningUnitSchedule(Configuration tableConf) {
		return tableConf.getBoolean(TableConfigOptions.SQL_SCHEDULE_RUNNING_UNIT_ENABLE);
	}

	/**
	 * Infer resource mode.
	 */
	public enum InferMode {
		NONE, ONLY_SOURCE, ALL
	}

	public static InferMode getInferMode(Configuration tableConf) {
		String config = tableConf.getString(
				TableConfigOptions.SQL_EXEC_INFER_RESOURCE_MODE);
		try {
			return InferMode.valueOf(config);
		} catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException("Infer mode can only be set: NONE, SOURCE or ALL.");
		}
	}
}
