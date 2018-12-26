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
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Metric provider 负责订阅metric，进行metric的数据的聚合.
 */
public interface MetricProvider {

	/**
	 * Agg type of metric.
	 */
	enum MetricAGG {
		SUM,
		MIN,
		MAX,
		AVG
	}

	/**
	 * Start service of the metric provider.
	 */
	void start();

	/**
	 * Stop service of the metric provider.
	 */
	void stop();

	/**
	 * Subscribe task manager metric.
	 * @param tmId
	 * @param metricName
	 * @param timeInterval
	 * @param timeAggType
	 * @param alias
	 */
	void subsribeTaskManagerMetric(
			String tmId,
			String metricName,
			long timeInterval,
			MetricAGG timeAggType,
			String alias);

	/**
	 * Subscribe task metric.
	 * @param jobId
	 * @param vertexId
	 * @param metricName
	 * @param subtaskAggType
	 * @param timeInterval
	 * @param timeAggType
	 * @param alias
	 */
	void subscribeTaskMetric(
			JobID jobId,
			JobVertexID vertexId,
			String metricName,
			MetricAGG subtaskAggType,
			long timeInterval,
			MetricAGG timeAggType,
			String alias);

	/**
	 * Get metric aggregated which should be subscribed ahead.
	 * @param jobId
	 * @param vertexID
	 * @param alias
	 * @return
	 */
	double getMetric(
			JobID jobId,
			JobVertexID vertexID,
			String alias);

}
