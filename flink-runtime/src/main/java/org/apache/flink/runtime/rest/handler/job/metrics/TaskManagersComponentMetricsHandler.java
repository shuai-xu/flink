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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that returns all taskmanager componentMetrics.
 */
public class TaskManagersComponentMetricsHandler extends AbstractComponentsMetricsHandler<TaskManagersComponentMetricsMessageParameters> {

	public TaskManagersComponentMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MetricFetcher metricFetcher) {

		super(localRestAddress, leaderRetriever, timeout, headers,
			TaskManagersComponentMetricsHeaders.getInstance(),
			metricFetcher);
	}

	@Nullable
	@Override
	protected Map<String, MetricStore.ComponentMetricStore> getComponentId2MetricStores(HandlerRequest<EmptyRequestBody,
		TaskManagersComponentMetricsMessageParameters> request,
		MetricStore metricStore, Set<String> requestedComponents) {
		Map<String, MetricStore.ComponentMetricStore> componentId2MetricStores = new HashMap<>();
		if (requestedComponents.isEmpty()){
			return componentId2MetricStores;
		} else {
			for (String componentId: requestedComponents) {
				MetricStore.ComponentMetricStore componentMetricStore = metricStore.getTaskManagerMetricStore(componentId);
				componentId2MetricStores.put(componentId, componentMetricStore);
			}
			return componentId2MetricStores;
		}
	}

}
