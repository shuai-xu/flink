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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Base class for {@link AccessExecutionGraph}  with history {@link AccessExecutionGraph} based REST handlers.
 *
 * @param <R> response type
 */
public abstract class AbstractExecutionGraphsHandler<R extends ResponseBody, M extends JobMessageParameters> extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, R, M> {

	private final ExecutionGraphCache executionGraphCache;

	private final Executor executor;

	protected AbstractExecutionGraphsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.executionGraphCache = Preconditions.checkNotNull(executionGraphCache);
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	protected CompletableFuture<R> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, M> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, gateway);
		CompletableFuture<EvictingBoundedList<ArchivedExecutionGraph>> historicalGraphsFuture = gateway.requestJobHistories(jobId, timeout);
		return executionGraphFuture.thenApplyAsync(
			graph -> {
				Tuple2<AccessExecutionGraph, EvictingBoundedList<ArchivedExecutionGraph>> graphTuple2 = new Tuple2<>();
				graphTuple2.setField(graph, 0);
				try {
					EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs = historicalGraphsFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
					graphTuple2.setField(historicalGraphs, 1);
				} catch (Exception ignore) {
					graphTuple2.setField(null, 1);
				}
				return graphTuple2;
			}, executor).thenApplyAsync(
			graphTuple2 -> {
				try {
					return handleRequest(request, graphTuple2.f0, graphTuple2.f1);
				} catch (RestHandlerException rhe) {
					throw new CompletionException(rhe);
				}
			}, executor)
			.exceptionally(throwable -> {
				throwable = ExceptionUtils.stripCompletionException(throwable);
				if (throwable instanceof FlinkJobNotFoundException) {
					throw new CompletionException(
						new NotFoundException(String.format("Job %s not found", jobId), throwable));
				} else {
					throw new CompletionException(throwable);
				}
			});
	}

	/**
	 * Called for each request after the corresponding {@link AccessExecutionGraph} has been retrieved from the
	 * {@link ExecutionGraphCache}.
	 *
	 * @param request for further information
	 * @param executionGraph for which the handler was called
	 * @return Response
	 * @throws RestHandlerException if the handler could not process the request
	 */
	protected abstract R handleRequest(HandlerRequest<EmptyRequestBody, M> request, AccessExecutionGraph executionGraph,
			EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs) throws RestHandlerException;

	protected static List<AccessExecutionJobVertex> getVertex(EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs, AccessExecutionJobVertex jobVertex){
		List<AccessExecutionJobVertex> vertexList = new ArrayList<>();
		vertexList.add(jobVertex);
		if (historicalGraphs != null) {
			for (ArchivedExecutionGraph graph: historicalGraphs) {
				if (graph != null) {
					AccessExecutionJobVertex v = graph.getJobVertex(jobVertex.getJobVertexId());
					if (v != null) {
						vertexList.add(v);
					}
				}
			}
		}
		return vertexList;
	}
}
