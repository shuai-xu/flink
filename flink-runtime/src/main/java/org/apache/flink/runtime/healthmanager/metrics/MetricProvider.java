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
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Metric provider will accept subscription request from plugins and fetch metric subscribed periodically.
 */
public interface MetricProvider {

	/**
	 * Start service of the metric provider.
	 */
	void open();

	/**
	 * Stop service of the metric provider.
	 */
	void close();

	/**
	 * Subscribe metric of all task manager belong a job.
	 * @param metricName   metric name to subscribe
	 * @param timeInterval timeline interval for agg
	 * @param timeAggType  agg type of timeline
	 *
	 * @return  a JobTMMetricSubscription containing the all agg result.
	 */
	JobTMMetricSubscription subscribeAllTMMetric(
			JobID jobID,
			String metricName,
			long timeInterval,
			TimelineAggType timeAggType);

	/**
	 * Subscribe metric of the given task manager.
	 * @param tmId           id of the task manager
	 * @param metricName     metric name to subscribe
	 * @param timeInterval   timeline interval for agg
	 * @param timeAggType    agg type of timeline
	 */
	TaskManagerMetricSubscription subscribeTaskManagerMetric(
			String tmId,
			String metricName,
			long timeInterval,
			TimelineAggType timeAggType);

	/**
	 * Subscribe task metric.
	 * @param jobId
	 * @param vertexId
	 * @param metricName
	 * @param subtaskAggType
	 * @param timeInterval
	 * @param timeAggType
	 */
	TaskMetricSubscription subscribeTaskMetric(
			JobID jobId,
			JobVertexID vertexId,
			String metricName,
			MetricAggType subtaskAggType,
			long timeInterval,
			TimelineAggType timeAggType);

	/**
	 * Unsubscribe a metric.
	 * @param subscription
	 */
	void unsubscribe(MetricSubscription subscription);

}
