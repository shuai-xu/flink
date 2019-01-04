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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
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

	/** all task metric subscription, format: [job id, [vertex id, [metric name, subscription]]]. */
	private Map<JobID, Map<JobVertexID, Map<String, List<TaskMetricSubscription>>>> taskMetricSubscriptions =
			new HashMap<>();
	/** all job tm metric subscription, format: [job id, [metric name, subscription]]. */
	private Map<JobID, Map<String, List<JobTMMetricSubscription>>> jobTMMetricSubscriptions = new HashMap<>();
	/** all tm metric subscription, format: [tm id, [metric name, subscription]]. */
	private Map<String, Map<String, List<TaskManagerMetricSubscription>>> tmMetricSubscriptions = new HashMap<>();

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
	public synchronized JobTMMetricSubscription subscribeAllTMMetric(
			JobID jobID, String metricName, long timeInterval, TimelineAggType timeAggType) {
		JobTMMetricSubscription metricSubscription = new JobTMMetricSubscription(
				jobID, metricName, timeAggType, timeInterval);
		jobTMMetricSubscriptions
				.computeIfAbsent(jobID, k -> new HashMap<>())
				.computeIfAbsent(metricName, k-> new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public synchronized TaskManagerMetricSubscription subscribeTaskManagerMetric(
			String tmId, String metricName, long timeInterval, TimelineAggType timeAggType) {
		TaskManagerMetricSubscription metricSubscription = new TaskManagerMetricSubscription(
				tmId, metricName, timeAggType, timeInterval);
		tmMetricSubscriptions.computeIfAbsent(tmId, k -> new HashMap<>())
				.computeIfAbsent(metricName, k -> new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public synchronized TaskMetricSubscription subscribeTaskMetric(
			JobID jobId, JobVertexID vertexId, String metricName, MetricAggType subtaskAggType,
			long timeInterval, TimelineAggType timeAggType) {
		TaskMetricSubscription metricSubscription = new TaskMetricSubscription(
				jobId, vertexId, subtaskAggType, metricName, timeAggType, timeInterval);
		taskMetricSubscriptions.computeIfAbsent(jobId, k -> new HashMap<>())
				.computeIfAbsent(vertexId, k -> new HashMap<>())
				.computeIfAbsent(metricName, k -> new LinkedList<>()).add(metricSubscription);
		return metricSubscription;
	}

	@Override
	public synchronized void unsubscribe(MetricSubscription subscription) {

		if (subscription instanceof TaskMetricSubscription) {
			TaskMetricSubscription taskMetricSubscription = (TaskMetricSubscription) subscription;
			Map<JobVertexID, Map<String, List<TaskMetricSubscription>>> subscriptionByJobVertex =
					taskMetricSubscriptions.get(taskMetricSubscription.getJobID());
			Map<String, List<TaskMetricSubscription>> subscriptionByMetricName =
					subscriptionByJobVertex.get(taskMetricSubscription.getJobVertexID());

			List<TaskMetricSubscription> subscriptionsOfOneMetric =
					subscriptionByMetricName.get(taskMetricSubscription.getMetricName());
			subscriptionsOfOneMetric.remove(taskMetricSubscription);
			if (subscriptionsOfOneMetric.isEmpty()) {
				subscriptionByMetricName.remove(taskMetricSubscription.getMetricName());
				if (subscriptionByMetricName.isEmpty()) {
					subscriptionByJobVertex.remove(taskMetricSubscription.getJobVertexID());
					if (subscriptionByJobVertex.isEmpty()) {
						taskMetricSubscriptions.remove(taskMetricSubscription.getJobID());
					}
				}
			}
		}

		if (subscription instanceof TaskManagerMetricSubscription) {
			TaskManagerMetricSubscription taskManagerMetricSubscription = (TaskManagerMetricSubscription) subscription;
			Map<String, List<TaskManagerMetricSubscription>> subscriptionByMetricName =
					tmMetricSubscriptions.get(taskManagerMetricSubscription.getTmId());
			List<TaskManagerMetricSubscription> subscriptionsOfOneMetric =
					subscriptionByMetricName.get(taskManagerMetricSubscription.getMetricName());
			subscriptionsOfOneMetric.remove(taskManagerMetricSubscription);
			if (subscriptionsOfOneMetric.isEmpty()) {
				subscriptionByMetricName.remove(taskManagerMetricSubscription.getMetricName());
				if (subscriptionByMetricName.isEmpty()) {
					tmMetricSubscriptions.remove(taskManagerMetricSubscription.getTmId());
				}
			}
		}

		if (subscription instanceof JobTMMetricSubscription) {
			JobTMMetricSubscription jobTMMetricSubscription = (JobTMMetricSubscription) subscription;
			Map<String, List<JobTMMetricSubscription>> subscriptionByMetricName =
					jobTMMetricSubscriptions.get(jobTMMetricSubscription.getJobID());
			List<JobTMMetricSubscription> subscriptionOfOneMetric =
					subscriptionByMetricName.get(jobTMMetricSubscription.getMetricName());
			subscriptionOfOneMetric.remove(jobTMMetricSubscription);
			if (subscriptionOfOneMetric.isEmpty()) {
				subscriptionByMetricName.remove(jobTMMetricSubscription.getMetricName());
				if (subscriptionByMetricName.isEmpty()) {
					jobTMMetricSubscriptions.remove(jobTMMetricSubscription.getJobID());
				}
			}
		}
	}

	private class MetricFetcher implements Runnable {

		@Override
		public void run() {
			synchronized (RestServerMetricProvider.this) {
				for (Map.Entry<JobID, Map<JobVertexID, Map<String, List<TaskMetricSubscription>>>> jobEntry : taskMetricSubscriptions.entrySet()) {
					for (Map.Entry<JobVertexID, Map<String, List<TaskMetricSubscription>>> vertexEntry : jobEntry.getValue().entrySet()) {
						Map<String, Map<Integer, Tuple2<Long, Double>>> values =
								restServerClient.getTaskMetrics(jobEntry.getKey(), vertexEntry.getKey(), vertexEntry.getValue().keySet());
						for (Map.Entry<String, List<TaskMetricSubscription>> metricEntry : vertexEntry.getValue().entrySet()) {
							for (TaskMetricSubscription subscription : metricEntry.getValue()) {
								subscription.addValue(values.get(metricEntry.getKey()));
							}
						}
					}
				}

				for (Map.Entry<JobID, Map<String, List<JobTMMetricSubscription>>> jobEntry : jobTMMetricSubscriptions.entrySet()) {
					Map<String, Map<String, Tuple2<Long, Double>>> values =
							restServerClient.getTaskManagerMetrics(jobEntry.getKey(), jobEntry.getValue().keySet());
					for (Map.Entry<String, List<JobTMMetricSubscription>> metricEntry : jobEntry.getValue().entrySet()) {
						for (JobTMMetricSubscription subscription : metricEntry.getValue()) {
							subscription.addValue(values.get(metricEntry.getKey()));
						}
					}
				}

				for (Map.Entry<String, Map<String, List<TaskManagerMetricSubscription>>> tmEntry : tmMetricSubscriptions.entrySet()) {
					Map<String, Map<String, Tuple2<Long, Double>>> values =
							restServerClient.getTaskManagerMetrics(Collections.singleton(tmEntry.getKey()), tmEntry.getValue().keySet());
					for (Map.Entry<String, List<TaskManagerMetricSubscription>> metricEntry : tmEntry.getValue().entrySet()) {
						for (TaskManagerMetricSubscription subscription : metricEntry.getValue()) {
							subscription.addValue(values.get(metricEntry.getKey()).get(tmEntry.getKey()));
						}
					}
				}
			}
		}
	}
}
