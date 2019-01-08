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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that returns vertex subtask componentMetrics.
 */
public class JobVertexSubtasksComponentMetricsHandler extends AbstractJobComponentMetricsHandler<JobVertexSubtasksComponentMetricsMessageParameters> {

	public JobVertexSubtasksComponentMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			ExecutionGraphCache executionGraphCache,
			MetricFetcher metricFetcher) {

		super(localRestAddress, leaderRetriever, timeout, headers,
			JobVertexSubtasksComponentMetricsHeaders.getInstance(),
			executionGraphCache, metricFetcher);
	}

	@Nullable
	@Override
	protected Map<String, MetricStore.ComponentMetricStore> getComponentId2MetricStores(HandlerRequest<EmptyRequestBody,
		JobVertexSubtasksComponentMetricsMessageParameters> request,
		MetricStore metricStore, Set<String> requestedComponents) {
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		final JobVertexID vertexId = request.getPathParameter(JobVertexIdPathParameter.class);
		Map<String, MetricStore.ComponentMetricStore> componentId2MetricStores = new HashMap<>();
		if (requestedComponents.isEmpty()){
			return componentId2MetricStores;
		} else {
			for (String componentId: requestedComponents) {
				int subtaskIndex = Integer.valueOf(componentId);
				MetricStore.ComponentMetricStore componentMetricStore = metricStore.getSubtaskMetricStore(jobId.toString(), vertexId.toString(), subtaskIndex);
				componentId2MetricStores.put(componentId, componentMetricStore);
			}
			return componentId2MetricStores;
		}
	}

	@Override
	protected Set<String> getAvailableComponents(AccessExecutionGraph executionGraph,
												HandlerRequest<EmptyRequestBody,
												JobVertexSubtasksComponentMetricsMessageParameters> request) throws NotFoundException{
		final JobVertexID vertexId = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(vertexId);

		if (jobVertex == null) {
			throw new NotFoundException(String.format("JobVertex %s not found", vertexId));
		} else {
			Set<String> subtaskIndexStr = new HashSet<>();
			for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
				subtaskIndexStr.add("" + vertex.getParallelSubtaskIndex());
			}
			return subtaskIndexStr;
		}
	}
}
