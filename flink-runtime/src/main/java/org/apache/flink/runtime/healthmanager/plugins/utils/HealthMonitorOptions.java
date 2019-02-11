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
	 * max parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MAX_RATIO =
			ConfigOptions.key("healthmonitor.parallelism.ratio.max").defaultValue(4.0);

	/**
	 * min parallelism per workload.
	 */
	public static final ConfigOption<Double> PARALLELISM_MIN_RATIO =
			ConfigOptions.key("healthmonitor.parallelism.ratio.min").defaultValue(2.0);

	/**
	 * parallelism check interval.
	 */
	public static final ConfigOption<Long> PARALLELISM_SCALE_INTERVAL =
			ConfigOptions.key("parallelism.scale.interval.ms").defaultValue(60 * 1000L);

	/**
	 * max partition per task.
	 */
	public static final ConfigOption<Integer> MAX_PARTITION_PER_TASK =
			ConfigOptions.key("parallelism.scale.max.partition.per.task").defaultValue(8);
}
