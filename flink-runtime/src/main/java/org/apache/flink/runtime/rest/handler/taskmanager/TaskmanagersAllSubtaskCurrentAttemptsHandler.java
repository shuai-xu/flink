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
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.TaskManagerExecutionVertexCache;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Request handler for all taskmanagers all SubtaskCurrentAttempts.
 */
public class TaskmanagersAllSubtaskCurrentAttemptsHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, TaskManagersExecutionVertexIdsInfo, EmptyMessageParameters> {

	private TaskManagerExecutionVertexCache taskManagerExecutionVertexCache;
	private ExecutionGraphCache executionGraphCache;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	public TaskmanagersAllSubtaskCurrentAttemptsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, TaskManagersExecutionVertexIdsInfo, EmptyMessageParameters> messageHeaders,
			TaskManagerExecutionVertexCache taskManagerExecutionVertexCache,
			ExecutionGraphCache executionGraphCache,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.taskManagerExecutionVertexCache = taskManagerExecutionVertexCache;
		this.executionGraphCache = executionGraphCache;
		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
	}

	@Override
	protected CompletableFuture<TaskManagersExecutionVertexIdsInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		try {
			ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);
			TaskManagersInfo ts = resourceManagerGateway.requestTaskManagerInfo(timeout).thenApply(TaskManagersInfo::new).get();
			Map<String, TaskManagerExecutionVertexIdsInfo> executionVertexIds;
			if (null != ts && ts.getTaskManagerInfos().size() > 0) {
				List<ResourceID> tmIds = ts.getTaskManagerInfos().stream().map(TaskManagerInfo::getResourceId).collect(Collectors.toList());
				executionVertexIds = new HashMap<>(tmIds.size());
				for (ResourceID tmId: tmIds) {
					TaskManagerExecutionVertexIdsInfo taskManagerExecutionVertexIdsInfo = new TaskManagerExecutionVertexIdsInfo(taskManagerExecutionVertexCache.getTaskManagerExecutionVertex(tmId, gateway, executionGraphCache));
					executionVertexIds.put(tmId.getResourceIdString(), taskManagerExecutionVertexIdsInfo);
				}
			} else {
				executionVertexIds = new HashMap<>();
			}
			CompletableFuture<TaskManagersExecutionVertexIdsInfo> r = new CompletableFuture<>();
			r.complete(new TaskManagersExecutionVertexIdsInfo(executionVertexIds));
			return r;
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}
}
