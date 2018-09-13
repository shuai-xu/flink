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
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableConfig;

/**
 * Deal with resource config.
 */
public class BatchExecResourceUtil {

	/**
	 * Sets the preferred buffer memory size for sort. It defines the upper limit for
	 * the sort.
	 */
	public static final String SQL_EXEC_SORT_PREFER_BUFFER_MEM = "sql.exec.sort.prefer-buffer-memory-mb";

	/**
	 * Sets the HashTable preferred memory for hashJoin operator. It defines the upper limit.
	 */
	public static final String SQL_EXEC_HASH_JOIN_TABLE_PREFER_MEM = "sql.exec.hash-join.table-prefer-memory-mb";

	/**
	 * Sets the table preferred memory size of hashAgg operator. It defines the upper limit.
	 */
	public static final String SQL_EXEC_HASH_AGG_TABLE_PREFER_MEM = "sql.exec.hash-agg.table-prefer-memory-mb";

	/**
	 * Sets min parallelism for operators.
	 */
	public static final String SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM = "sql.exec.infer-resource.operator.min-parallelism";

	/**
	 * Maybe the infer's reserved manager mem is too small, so this setting is lower limit for
	 * the infer's manager mem.
	 */
	public static final String SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB = "sql.exec.infer-resource.operator.min-memory-mb";

	/**
	 * Sets reserve-relative-prefer mem ratio.
	 */
	public static final String SQL_EXEC_INFER_RESERVE_RELATIVE_PREFER_MEM_RATIO = "sql.exec.infer.reserve-relative-prefer.mem.ratio";


	/**
	 * How many Bytes per MB.
	 */
	public static final long SIZE_IN_MB =  1024L * 1024;

	public static double getCpu(TableConfig tConfig) {
		return tConfig.getParameters().getDouble(
				TableConfig.SQL_EXEC_DEFAULT_CPU(),
				TableConfig.SQL_EXEC_DEFAULT_CPU_DEFAULT());
	}

	/**
	 * Gets default parallelism of operator.
	 * @param tConfig TableConfig.
	 * @return default parallelism of operator.
	 */
	public static int getOperatorDefaultParallelism(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_DEFAULT_PARALLELISM(),
				TableConfig.SQL_EXEC_DEFAULT_PARALLELISM_DEFAULT());
	}

	/**
	 * Gets default resourceSpec for a operator that has no specific resource need.
	 * For converter rel.
	 * @param tConfig TableConfig.
	 * @return default resourceSpec for a operator that has no specific resource need.
	 */
	public static ResourceSpec getDefaultResourceSpec(TableConfig tConfig) {
		ResourceSpec.Builder builder = new ResourceSpec.Builder();
		builder.setCpuCores(getCpu(tConfig));
		builder.setHeapMemoryInMB(getDefaultHeapMem(tConfig));
		return builder.build();
	}

	/**
	 * Gets resourceSpec which specific heapMemory size. For sink rel.
	 * @param tConfig TableConfig.
	 * @param heapMemory the specific heapMemory size.
	 * @return resourceSpec which specific heapMemory size.
	 */
	public static ResourceSpec getResourceSpec(
			TableConfig tConfig,
			int heapMemory) {
		ResourceSpec.Builder builder = new ResourceSpec.Builder();
		builder.setCpuCores(getCpu(tConfig));
		builder.setHeapMemoryInMB(heapMemory);
		return builder.build();
	}

	public static int getDefaultHeapMem(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_DEFAULT_MEM(),
				64);
	}

	/**
	 * Gets the config managedMemory for sort buffer.
	 * @param tConfig TableConfig.
	 * @return the config managedMemory for sort buffer.
	 */
	public static int getSortBufferManagedMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_SORT_BUFFER_MEM(),
				TableConfig.SQL_EXEC_SORT_BUFFER_MEM_DEFAULT());
	}

	/**
	 * Gets the config managedMemory for external buffer.
	 * @param tConfig TableConfig.
	 * @return the config managedMemory for external buffer.
	 */
	public static int getExternalBufferManagedMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_EXTERNAL_BUFFER_MEM(),
				TableConfig.SQL_EXEC_EXTERNAL_BUFFER_MEM_DEFAULT());
	}

	/**
	 * Gets the config managedMemory for hashJoin table.
	 * @param tConfig TableConfig.
	 * @return the config managedMemory for hashJoin table.
	 */
	public static int getHashJoinTableManagedMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM(),
				TableConfig.SQL_EXEC_HASH_JOIN_TABLE_MEM_DEFAULT());
	}

	/**
	 * Gets the config memory for source.
	 * @param tConfig TableConfig.
	 * @return the config memory for source.
	 */
	public static int getSourceMem(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_SOURCE_MEM(),
				TableConfig.SQL_EXEC_SOURCE_MEM_DEFAULT());
	}

	/**
	 * Gets the config memory for sink.
	 * @param tConfig TableConfig.
	 * @return the config memory for sink.
	 */
	public static int getSinkMem(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_SINK_MEM(),
				TableConfig.SQL_EXEC_SINK_MEM_DEFAULT());
	}

	/**
	 * Gets the config managedMemory for hashAgg.
	 * @param tConfig TableConfig.
	 * @return the config managedMemory for hashAgg.
	 */
	public static int getHashAggManagedMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM(),
				TableConfig.SQL_EXEC_HASH_AGG_TABLE_MEM_DEFAULT());
	}

	/**
	 * Gets the config managedMemory for hashAgg.
	 * @param tConfig TableConfig.
	 * @return the config managedMemory for hashAgg.
	 */
	public static int getWindowAggBufferLimitSize(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE(),
				TableConfig.SQL_EXEC_WINDOW_AGG_BUFFER_LIMIT_SIZE_DEFAULT());
	}


	/**
	 * Gets the config row count that one partition processes.
	 * @param tConfig TableConfig.
	 * @return the config row count that one partition processes.
	 */
	public static long getRelCountPerPartition(TableConfig tConfig) {
		return tConfig.getParameters().getLong(
				TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_ROWS_PER_PARTITION_DEFAULT()
		);
	}

	/**
	 * Gets the config data size that one partition processes.
	 * @param tConfig TableConfig.
	 * @return the config data size that one partition processes.
	 */
	public static long getSourceSizePerPartition(TableConfig tConfig) {
		return tConfig.getParameters().getLong(
				TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MB_PER_PARTITION_DEFAULT()
		);
	}

	/**
	 * Gets the config max num of source parallelism.
	 * @param tConfig TableConfig.
	 * @return the config max num of source parallelism.
	 */
	public static int getSourceMaxParallelism(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_SOURCE_MAX_PARALLELISM_DEFAULT()
		);
	}

	/**
	 * Gets the config max num of operator parallelism.
	 * @param tConfig TableConfig.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMaxParallelism(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_PARALLELISM_DEFAULT()
		);
	}

	/**
	 * Gets the config min num of operator parallelism.
	 * @param tConfig TableConfig.
	 * @return the config max num of operator parallelism.
	 */
	public static int getOperatorMinParallelism(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_PARALLELISM,
				1
		);
	}

	/**
	 * Gets the preferred managedMemory for sort buffer.
	 * @param tConfig TableConfig.
	 * @return the prefer managedMemory for sort buffer.
	 */
	public static int getSortBufferManagedPreferredMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				SQL_EXEC_SORT_PREFER_BUFFER_MEM,
				getSortBufferManagedMemory(tConfig));
	}

	/**
	 * Gets the preferred managedMemory for hashJoin table.
	 * @param tConfig TableConfig.
	 * @return the preferred managedMemory for hashJoin table.
	 */
	public static int getHashJoinTableManagedPreferredMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				SQL_EXEC_HASH_JOIN_TABLE_PREFER_MEM,
				getHashJoinTableManagedMemory(tConfig));
	}

	/**
	 * Gets the preferred managedMemory for hashAgg.
	 * @param tConfig TableConfig.
	 * @return the preferred managedMemory for hashAgg.
	 */
	public static int getHashAggManagedPreferredMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				SQL_EXEC_HASH_AGG_TABLE_PREFER_MEM,
				getHashAggManagedMemory(tConfig));
	}

	/**
	 * get the reserved and preferred mem by the infer mem. And they should be between the maximum
	 * and the minimum mem.
	 *
	 * @param tConfig TableConfig.
	 * @param memCostInMB the infer mem of per partition.
	 */
	public static Tuple2<Integer, Integer> reviseAndGetInferManagedMem(TableConfig tConfig, int memCostInMB) {
		double relativeRatio = tConfig.getParameters().getDouble(
				SQL_EXEC_INFER_RESERVE_RELATIVE_PREFER_MEM_RATIO,
				2.0);

		int maxMem = tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_OPERATOR_MAX_MEMORY_MB_DEFAULT());

		int minMem = tConfig.getParameters().getInteger(
				SQL_EXEC_INFER_RESOURCE_OPERATOR_MIN_MEMORY_MB,
				32);

		int preferMemCostInMB = (int) (memCostInMB * relativeRatio);

		int reservedMem = Math.max(Math.min(maxMem, memCostInMB), minMem);

		int preferredMem = Math.max(Math.min(maxMem, preferMemCostInMB), reservedMem);

		return new Tuple2<>(reservedMem, preferredMem);
	}

	/**
	 * Gets the managedMemory for per-allocating.
	 */
	public static int getPerRequestManagedMemory(TableConfig tConfig) {
		return tConfig.getParameters().getInteger(
				TableConfig.SQL_EXEC_PER_REQUEST_MEM(),
				TableConfig.SQL_EXEC_PER_REQUEST_MEM_DEFAULT());
	}

	/**
	 * Whether to enable schedule with runningUnit.
	 */
	public static boolean enableRunningUnitSchedule(TableConfig tConfig) {
		return tConfig.getParameters().getBoolean(
				TableConfig.SQL_SCHEDULE_RUNNING_UNIT_ENABLE(),
				TableConfig.SQL_SCHEDULE_RUNNING_UNIT_ENABLE_DEFAULT());
	}

	/**
	 * Gets total resource limit for a runningUnit.
	 */
	public static Tuple2<Double, Long> getRunningUnitResourceLimit(TableConfig tConfig) {
		String resource = tConfig.getParameters().getString(
				TableConfig.SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU_MEM(),
				null
		);
		if (resource == null) {
			return null;
		}
		String[] s = resource.split(",");
		if (s.length != 2) {
			throw new IllegalArgumentException(TableConfig.SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU_MEM() + " set illegal, need: double, long");
		}
		try {
			double cpu = Double.valueOf(s[0].trim());
			long mem = Long.valueOf(s[1].trim());
			return new Tuple2<>(cpu, mem);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException(TableConfig.SQL_RESOURCE_RUNNING_UNIT_TOTAL_CPU_MEM() + " set illegal, need: double, long", ex);
		}
	}

	/**
	 * Infer resource mode.
	 */
	public enum InferMode {
		NONE, ONLY_SOURCE, ALL
	}

	public static InferMode getInferMode(TableConfig tConfig) {
		String config = tConfig.getParameters().getString(TableConfig.SQL_EXEC_INFER_RESOURCE_MODE(),
				TableConfig.SQL_EXEC_INFER_RESOURCE_MODE_DEFAULT());
		try {
			return InferMode.valueOf(config);
		} catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException("Infer mode can only be set: NONE, SOURCE or ALL.");
		}
	}

	public static int getManagedMemory(ResourceSpec resourceSpec) {
		Object managedMemory = resourceSpec.getExtendedResources().get(ResourceSpec.MANAGED_MEMORY_NAME);
		if (managedMemory == null) {
			return 0;
		}
		managedMemory = ((Resource) managedMemory).getValue();
		try {
			// we need a int value here, cast to double first in order to accept double config val.
			return  ((Double) managedMemory).intValue();
		} catch (ClassCastException ex) {
			return 0;
		}
	}
}
