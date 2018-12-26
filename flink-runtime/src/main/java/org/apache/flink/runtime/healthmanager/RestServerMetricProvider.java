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

package org.apache.flink.runtime.healthmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.concurrent.ScheduledExecutorService;

/**
 * MetricProvider based on Rest Server.
 */
public class RestServerMetricProvider implements MetricProvider {

	private Configuration config;
	private RestServerClient restServerClient;
	private ScheduledExecutorService scheduledExecutorService;

	public RestServerMetricProvider(
			Configuration config,
			RestServerClient restServerClient,
			ScheduledExecutorService scheduledExecutorService) {

		this.config = config;
		this.restServerClient = restServerClient;
		this.scheduledExecutorService = scheduledExecutorService;

	}

	@Override
	public void start() {
	}

	@Override
	public void stop() {
	}

	@Override
	public void subsribeTaskManagerMetric(String tmId, String metricName, long timeInterval,
			MetricAGG timeAggType, String alias) {

	}

	@Override
	public void subscribeTaskMetric(JobID jobId, JobVertexID vertexId, String metricName,
			MetricAGG subtaskAggType, long timeInterval,
			MetricAGG timeAggType, String alias) {

	}

	@Override
	public double getMetric(JobID jobId, JobVertexID vertexID, String alias) {
		return 0;
	}

	private class MetricFether implements Runnable {

		@Override
		public void run() {

		}
	}
}
