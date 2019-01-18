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

package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.UpdatingJobRequest;
import org.apache.flink.runtime.update.JobUpdateRequest;
import org.apache.flink.runtime.update.action.JobUpdateAction;
import org.apache.flink.runtime.update.action.JobVertexRescaleAction;
import org.apache.flink.runtime.update.action.JobVertexResourcesUpdateAction;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Rest handler to trigger and poll the updating of a running job.
 */
public class UpdatingJobHandlers extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Acknowledge> {

	/**
	 * Handler which triggers the updating of the specified job.
	 */
	public class UpdatingTriggerHandler extends TriggerHandler<RestfulGateway, UpdatingJobRequest, JobMessageParameters> {

		private Time timeout;
		public UpdatingTriggerHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders) {
			super(
				localRestAddress,
				leaderRetriever,
				timeout,
				responseHeaders,
				UpdatingTriggerHeaders.getInstance());
			this.timeout = timeout;
		}

		@Override
		protected CompletableFuture<Acknowledge> triggerOperation(HandlerRequest<UpdatingJobRequest, JobMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			final Map<String, UpdatingJobRequest.VertexResource> vertexParallelismResources = request.getRequestBody().getVertexParallelismResource();

			List<JobUpdateAction> actions = new ArrayList<>();
			if (vertexParallelismResources != null && !vertexParallelismResources.isEmpty()) {
				for (Map.Entry<String, UpdatingJobRequest.VertexResource> vertexParallelismResource: vertexParallelismResources.entrySet()){
					JobVertexID jobVertexID = JobVertexID.fromHexString(vertexParallelismResource.getKey());
					UpdatingJobRequest.VertexResource vertexResource = vertexParallelismResource.getValue();
					ResourceSpec newResourceSpec = vertexResource.getResource().convertToResourceSpec();
					int parallelism = vertexResource.getParallelism();
					actions.add(new JobVertexResourcesUpdateAction(jobVertexID, newResourceSpec));
					actions.add(new JobVertexRescaleAction(jobVertexID, parallelism));
				}
			}
			JobUpdateRequest updateRequestrequest = new JobUpdateRequest(actions);
			final CompletableFuture<Acknowledge> updatingFuture = gateway.updateJob(jobId,
				updateRequestrequest,
				timeout);

			return updatingFuture;
		}

		@Override
		protected AsynchronousJobOperationKey createOperationKey(HandlerRequest<UpdatingJobRequest, JobMessageParameters> request) {
			final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
			return AsynchronousJobOperationKey.of(new TriggerId(), jobId);
		}
	}
}
