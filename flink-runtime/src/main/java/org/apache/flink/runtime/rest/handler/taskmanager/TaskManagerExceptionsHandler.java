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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExceptionsInfos;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler which get exception from TaskManager.
 */
public class TaskManagerExceptionsHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, TaskManagerExceptionsInfos, EmptyMessageParameters> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	public TaskManagerExceptionsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, TaskManagerExceptionsInfos, EmptyMessageParameters> messageHeaders,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
	}

	@Override
	protected CompletableFuture<TaskManagerExceptionsInfos> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);
		return resourceManagerGateway.requestTaskManagerExceptions(timeout).thenApply(time2Exceptions -> {
				Map<Long, TaskManagerExceptionsInfos.TaskManagerException> taskmanagerExceptionsTmp = new HashMap<>();
				if (null != time2Exceptions) {
					for (Map.Entry<Long, Tuple2<ResourceID, Exception>> time2Exception : time2Exceptions.entrySet()) {
						TaskManagerExceptionsInfos.TaskManagerException te = new TaskManagerExceptionsInfos.TaskManagerException(time2Exception.getValue().f0, time2Exception.getValue().f1);
						taskmanagerExceptionsTmp.put(time2Exception.getKey(), te);
					}
				}
				return new TaskManagerExceptionsInfos(taskmanagerExceptionsTmp);
		});
	}
}
