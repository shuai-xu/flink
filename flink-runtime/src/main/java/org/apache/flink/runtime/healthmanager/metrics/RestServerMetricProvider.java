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

package org.apache.flink.runtime.healthmanager.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * MetricProvider based on Rest Server.
 */
public class RestServerMetricProvider implements MetricProvider {

	private static final ConfigOption<Long> METRIC_FETCH_INTERVAL_OPTION =
			ConfigOptions.key("metric.provider.fetch.interval.ms").defaultValue(20_000L);

	private Configuration config;
	private RestServerClient restServerClient;
	private ScheduledExecutorService scheduledExecutorService;

	private ScheduledFuture fetchTaskHandler;

	private Map<String, List<TaskMetricSubscription>> taskMetrics = new HashMap<>();
	private Map<String, List<TaskManagerMetricSubscription>> taskManagerMetrics = new HashMap<>();
	private Map<String, List<JobTMMetricSubscription>> allTMMetrics = new HashMap<>();

	public RestServerMetricProvider(
			Configuration config,
			RestServerClient restServerClient,
			ScheduledExecutorService scheduledExecutorService) {

		this.config = config;
		this.restServerClient = restServerClient;
		this.scheduledExecutorService = scheduledExecutorService;

	}

	@Override
	public void open() {
		long fetchIntervalMS = this.config.getLong(METRIC_FETCH_INTERVAL_OPTION);
		fetchTaskHandler = this.scheduledExecutorService.scheduleAtFixedRate(
				new MetricFetcher(), fetchIntervalMS, fetchIntervalMS, TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		if (fetchTaskHandler != null) {
			fetchTaskHandler.cancel(true);
		}
	}

	@Override
	public JobTMMetricSubscription subscribeAllTMMetric(JobID jobID, String metricName, long timeInterval,
			TimelineAggType timeAggType) {
		JobTMMetricSubscription metricSubscription = new JobTMMetricSubscription(
				jobID, metricName, timeAggType, timeInterval);
		allTMMetrics.putIfAbsent(metricName, new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public TaskManagerMetricSubscription subscribeTaskManagerMetric(String tmId, String metricName,
			long timeInterval, TimelineAggType timeAggType) {
		TaskManagerMetricSubscription metricSubscription = new TaskManagerMetricSubscription(
				tmId, metricName, timeAggType, timeInterval);
		taskManagerMetrics.putIfAbsent(metricName, new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public TaskMetricSubscription subscribeTaskMetric(JobID jobId, JobVertexID vertexId,
			String metricName, MetricAggType subtaskAggType, long timeInterval,
			TimelineAggType timeAggType) {
		TaskMetricSubscription metricSubscription = new TaskMetricSubscription(
				jobId, vertexId, subtaskAggType, metricName, timeAggType, timeInterval);
		taskMetrics.putIfAbsent(metricName, new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public void unsubscribe(MetricSubscription subscription) {

		if (allTMMetrics.containsKey(subscription.getMetricName())) {
			boolean removed = allTMMetrics.get(subscription.getMetricName()).remove(subscription);
			if (removed) {
				if (allTMMetrics.get(subscription.getMetricName()).size() == 0) {
					allTMMetrics.remove(subscription.getMetricName());
				}
			}
		}

		if (taskManagerMetrics.containsKey(subscription.getMetricName())) {
			boolean removed = taskManagerMetrics.get(subscription.getMetricName()).remove(subscription);
			if (removed) {
				if (taskManagerMetrics.get(subscription.getMetricName()).size() == 0) {
					taskManagerMetrics.remove(subscription.getMetricName());
				}
			}
		}

		if (taskMetrics.containsKey(subscription.getMetricName())) {
			boolean removed = taskMetrics.get(subscription.getMetricName()).remove(subscription);
			if (removed) {
				if (taskMetrics.get(subscription.getMetricName()).size() == 0) {
					taskMetrics.remove(subscription.getMetricName());
				}
			}
		}
	}

	private class MetricFetcher implements Runnable {

		@Override
		public void run() {

		}
	}
}
