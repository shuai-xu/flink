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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.TaskManagerExecutionVertexCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for the taskmanager all SubtaskCurrentAttempts.
 */
public class TaskmanagerAllSubtaskCurrentAttemptsHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, TaskManagerExecutionVertexIdsInfo, TaskManagerMessageParameters> {

	private TaskManagerExecutionVertexCache taskManagerExecutionVertexCache;
	private ExecutionGraphCache executionGraphCache;
	public TaskmanagerAllSubtaskCurrentAttemptsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, TaskManagerExecutionVertexIdsInfo, TaskManagerMessageParameters> messageHeaders,
			TaskManagerExecutionVertexCache taskManagerExecutionVertexCache,
			ExecutionGraphCache executionGraphCache) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.taskManagerExecutionVertexCache = taskManagerExecutionVertexCache;

		this.executionGraphCache = executionGraphCache;
	}

	@Override
	protected CompletableFuture<TaskManagerExecutionVertexIdsInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerResourceId = request.getPathParameter(TaskManagerIdPathParameter.class);
		try {
			CompletableFuture<TaskManagerExecutionVertexIdsInfo> r = new CompletableFuture<>();
			r.complete(new TaskManagerExecutionVertexIdsInfo(
				taskManagerExecutionVertexCache.getTaskManagerExecutionVertex(taskManagerResourceId, gateway, executionGraphCache)
			));
			return r;
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}
}
