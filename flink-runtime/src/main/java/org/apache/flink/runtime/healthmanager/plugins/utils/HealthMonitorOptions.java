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

	/**
	 * max parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MAX_RATIO =
			ConfigOptions.key("parallelism.scale.ratio.max").defaultValue(4.0);

	/**
	 * min parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MIN_RATIO =
			ConfigOptions.key("parallelism.scale.ratio.min").defaultValue(2.0);

	/**
	 * parallelism check interval.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_INTERVAL =
			ConfigOptions.key("parallelism.scale.interval.ms").defaultValue(3 * 60 * 1000L);

	/**
	 * parallelism stable time.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_STABLE_TIME =
		ConfigOptions.key("parallelism.scale.stable-time.ms").defaultValue(6 * 60 * 1000L);

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
			ConfigOptions.key("parallelism.scale.timeout.ms").defaultValue(180000L);

	/**
	 * resource scale ratio.
	 */
	public static final ConfigOption<Double> RESOURCE_SCALE_RATIO =
			ConfigOptions.key("resource.scale.ratio").defaultValue(1.5);

	/**
	 * resource check interval.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_INTERVAL =
		ConfigOptions.key("resource.scale.interval.ms").defaultValue(180 * 1000L);

	/**
	 * resource stable time.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_STABLE_TIME =
		ConfigOptions.key("resource.scale.stable-time.ms").defaultValue(3 * 60 * 1000L);

	/**
	 * resource opportunistic action delay.
	 */
	public static final ConfigOption<Long> RESOURCE_OPPORTUNISTIC_ACTION_DELAY =
		ConfigOptions.key("resource.opportunistic-action.delay.second").defaultValue(24 * 60 * 60 * 1000L);

	/**
	 * resource scale time out.
	 */
	public static final ConfigOption<Long> RESOURCE_SCALE_TIME_OUT =
		ConfigOptions.key("resource.scale.timeout.ms").defaultValue(180000L);
}
