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

package org.apache.flink.runtime.healthmanager.plugins.utils;

/**
 * Metric names used in plugins.
 */
public class MetricNames {
	public static final String SOURCE_DELAY = "fetched_delay";
	public static final String SOURCE_PARTITION_COUNT = "partitionCount";
	public static final String SOURCE_PARTITION_LATENCY_COUNT = "partitionLatency.count";
	public static final String SOURCE_PARTITION_LATENCY_SUM = "partitionLatency.sum";
	public static final String SOURCE_LATENCY_COUNT = "sourceLatency.count";
	public static final String SOURCE_LATENCY_SUM = "sourceLatency.sum";
	public static final String SOURCE_PROCESS_LATENCY_COUNT = "sourceProcessLatency.count";
	public static final String SOURCE_PROCESS_LATENCY_SUM = "sourceProcessLatency.sum";
	public static final String TASK_LATENCY_COUNT = "taskLatency.count";
	public static final String TASK_LATENCY_SUM = "taskLatency.sum";
	public static final String TASK_INPUT_COUNT = "numRecordsReceived";
	public static final String TASK_OUTPUT_COUNT = "numRecordsSent";
	public static final String WAIT_OUTPUT_COUNT = "waitOutput.count";
	public static final String WAIT_OUTPUT_SUM = "waitOutput.sum";
	public static final String TASK_INIT_TIME = "taskInitTime.ms";

	public static final String TM_CPU_CAPACITY = "Status.ProcessTree.CPU.Allocated";
	public static final String TM_CPU_USAGE = "Status.ProcessTree.CPU.Usage";
	public static final String TM_MEM_CAPACITY = "Status.ProcessTree.Memory.Allocated";
	public static final String TM_MEM_USAGE_TOTAL = "Status.ProcessTree.Memory.RSS";
	public static final String TM_MEM_HEAP_USED = "Status.JVM.Memory.Heap.Used";
	public static final String TM_MEM_NON_HEAP_USED = "Status.JVM.Memory.NonHeap.Used";
	public static final String FULL_GC_COUNT_METRIC = "Status.JVM.GarbageCollector.ConcurrentMarkSweep.Count";
}
