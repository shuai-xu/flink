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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Config options for health monitor plugins.
 */
public class HealthMonitorOptions {

	/**
	 * enable health manager.
	 */
	public static final ConfigOption<Boolean> ENABLE_HEALTH_MANAGER =
			ConfigOptions.key("healthmanager.enabled").defaultValue(false);

	public static final ConfigOption<Boolean> ENABLE_PARALLELISM_RESCALE =
			ConfigOptions.key("healthmanager.parallelism.enabled").defaultValue(true);

	public static final ConfigOption<Boolean> ENABLE_RESOURCE_RESCALE =
		ConfigOptions.key("healthmanager.resource.enabled").defaultValue(true);

	/**
	 * max parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MAX_RATIO =
			ConfigOptions.key("parallelism.scale.ratio.max").defaultValue(6.0);

	/**
	 * min parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MIN_RATIO =
			ConfigOptions.key("parallelism.scale.ratio.min").defaultValue(3.0);

	/**
	 * parallelism rescale ratio for massive timers detected.
	 */
	public static final ConfigOption<Double> TIMER_SCALE_RATIO =
		ConfigOptions.key("timer.scale.ratio").defaultValue(2.0);

	/**
	 * parallelism check interval.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_INTERVAL =
			ConfigOptions.key("parallelism.scale.interval.ms").defaultValue(10 * 60 * 1000L);

	/**
	 * parallelism stable time.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_STABLE_TIME =
		ConfigOptions.key("parallelism.scale.stable-time.ms").defaultValue(6 * 60 * 1000L);

	/**
	 * interval threshold to last checkpoint while scaling down.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_CHECKPOINT_THRESHOLD =
			ConfigOptions.key("parallelism.scale.checkpoint.threshold.ms").defaultValue(30 * 1000L);
	/**
	 * parallel source max partition per task.
	 */
	public static final ConfigOption<Integer> MAX_PARTITION_PER_TASK =
			ConfigOptions.key("parallelism.scale.parallel-source.max.partition.per.task").defaultValue(8);

	/**
	 * parallel source reserved parallelism ratio.
	 */
	public static final ConfigOption<Double> RESERVED_PARALLELISM_RATIO =
			ConfigOptions.key("parallelism.scale.parallel-source.reserved.parallelism.ratio").defaultValue(1.2);

	/**
	 * parallelism scale time out.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_TIME_OUT =
			ConfigOptions.key("parallelism.scale.timeout.ms").defaultValue(10 * 60 * 1000L);

	public static final ConfigOption<Long> PARALLELISM_SCALE_STATE_SIZE_THRESHOLD =
			ConfigOptions.key("parallelism.scale.state.size.threshold").defaultValue(10L * 1024 * 1024 * 1024);

	/**
	 * resource scale up ratio.
	 */
	public static final ConfigOption<Double> RESOURCE_SCALE_UP_RATIO =
			ConfigOptions.key("resource.scale-up.ratio").defaultValue(1.5);

	/**
	 * resource scale down ratio.
	 */
	public static final ConfigOption<Double> RESOURCE_SCALE_DOWN_RATIO =
		ConfigOptions.key("resource.scale-down.ratio").defaultValue(1.2);

	/**
	 * resource check interval.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_INTERVAL =
		ConfigOptions.key("resource.scale.interval.ms").defaultValue(180 * 1000L);

	/**
	 * resource scale down check interval.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_DOWN_WAIT_TIME =
		ConfigOptions.key("resource.scale-down.wait-time.ms").defaultValue(24 * 60 * 60 * 1000L);

	/**
	 * resource stable time.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_STABLE_TIME =
		ConfigOptions.key("resource.scale.stable-time.ms").defaultValue(3 * 60 * 1000L);

	/**
	 * resource opportunistic action delay.
	 */
	public static final ConfigOption<Long> RESOURCE_OPPORTUNISTIC_ACTION_DELAY =
		ConfigOptions.key("resource.opportunistic-action.delay.ms").defaultValue(24 * 60 * 60 * 1000L);

	/**
	 * resource scale time out.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_TIME_OUT =
		ConfigOptions.key("resource.scale.timeout.ms").defaultValue(10 * 60 * 1000L);

	public static final ConfigOption<Double> FRAMEWORK_MEMORY_RATIO =
		ConfigOptions.key("resource.memory.framework.ratio").defaultValue(0.2);

	public static final ConfigOption<Double> RESOURCE_SCALE_MIN_DIFF_RATIO =
		ConfigOptions.key("resource.scale.min-diff.ratio").defaultValue(0.1);

	public static final ConfigOption<Double> RESOURCE_SCALE_MIN_DIFF_CPU =
		ConfigOptions.key("resource.scale.min-diff.cpu.core").defaultValue(0.1);

	public static final ConfigOption<Integer> RESOURCE_SCALE_MIN_DIFF_NATIVE_MEM =
		ConfigOptions.key("resource.scale.min-diff.mem.mb").defaultValue(100);

	public static final ConfigOption<Double> PARALLELISM_SCALE_MIN_DIFF_RATIO =
		ConfigOptions.key("parallelism.scale.min-diff.ratio").defaultValue(0.1);
}
