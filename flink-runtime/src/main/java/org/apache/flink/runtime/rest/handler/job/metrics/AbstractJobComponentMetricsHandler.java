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
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentsMetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that returns vertex subtask componentMetrics.
 */
public abstract class AbstractJobComponentMetricsHandler<M extends MessageParameters> extends AbstractComponentsMetricsHandler<M> {

	private final ExecutionGraphCache executionGraphCache;

	public AbstractJobComponentMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody, ComponentsMetricCollectionResponseBody, M> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			MetricFetcher metricFetcher) {
		super(localRestAddress, leaderRetriever, timeout, headers, messageHeaders, metricFetcher);
		this.executionGraphCache = executionGraphCache;
	}

	@Override
	protected Set<String> getAvailableComponents(Set<String> requestedComponenets, RestfulGateway gateway,
												HandlerRequest<EmptyRequestBody, M> request)  throws NotFoundException {
		if (requestedComponenets.isEmpty()) {
			JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			AccessExecutionGraph executionGraph = null;
			try {
				executionGraph = executionGraphCache.getExecutionGraph(jobId, gateway).get();
			} catch (Exception ignore) {
			}
			return getAvailableComponents(executionGraph, request);
		} else {
			return requestedComponenets;
		}
	}

	protected abstract Set<String> getAvailableComponents(AccessExecutionGraph executionGraph, HandlerRequest<EmptyRequestBody, M> request) throws NotFoundException;
}
