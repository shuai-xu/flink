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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.TaskMetricSubscription;

/**
 * Utils for validation of metrics.
 */
public class MetricUtils {

	/**
	 * Get start execute time of the job.
	 */
	public static long getStartExecuteTime(HealthMonitor monitor) {

		long startTime = Long.MIN_VALUE;

		// when all task start running, the job go into start time.
		RestServerClient.JobStatus status;
		try {
			status = monitor.getRestServerClient().getJobStatus(monitor.getJobID());
		} catch (Exception e) {
			// job not stable.
			return Long.MAX_VALUE;
		}
		for (Tuple2<Long, ExecutionState> state : status.getTaskStatus().values()) {
			if (!state.f1.equals(ExecutionState.RUNNING)) {
				// job not stable.
				return Long.MAX_VALUE;
			}
			if (startTime < state.f0) {
				startTime = state.f0;
			}
		}
		return startTime;
	}

	/**
	 * Validate current value of task metric.
	 */
	public static boolean validateTaskMetric(
			HealthMonitor monitor, long validInterval, TaskMetricSubscription ... metrics) {
		long now = System.currentTimeMillis();
		for (TaskMetricSubscription metric : metrics) {
			Tuple2<Long, Double> val = metric.getValue();
			if (val == null || val.f0 < monitor.getJobStartExecutionTime() ||
					now - val.f0 > validInterval) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Validate current value of tm metric.
	 */
	public static boolean validateTmMetric(HealthMonitor monitor, long validInterval, Tuple2<Long, Double>... metrics) {
		long now = System.currentTimeMillis();
		for (Tuple2<Long, Double> metric : metrics) {
			if (metric == null || metric.f0 < monitor.getJobStartExecutionTime() ||
				validInterval > 1 && now - metric.f0 > validInterval) {
				return false;
			}
		}
		return true;
	}
}
