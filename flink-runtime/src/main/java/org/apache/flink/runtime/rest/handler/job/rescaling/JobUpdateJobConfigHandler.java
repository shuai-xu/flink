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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobUpdatingConfigStatus;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.UpdatingJobConfigRequest;
import org.apache.flink.runtime.update.JobUpdateRequest;
import org.apache.flink.runtime.update.action.JobConfigUpdateAction;
import org.apache.flink.runtime.update.action.JobUpdateAction;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * update job config. header {@link JobUpdateJobConfigHeaders}.
 */
public class JobUpdateJobConfigHandler
	extends AbstractRestHandler<RestfulGateway, UpdatingJobConfigRequest, JobUpdatingConfigStatus, JobMessageParameters> {

	public JobUpdateJobConfigHandler(
		final CompletableFuture<String> localRestAddress,
		final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		final Time timeout,
		final Map<String, String> responseHeaders,
		final MessageHeaders<UpdatingJobConfigRequest, JobUpdatingConfigStatus, JobMessageParameters> headers) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			headers);
	}

	@Override
	protected CompletableFuture<JobUpdatingConfigStatus> handleRequest(@Nonnull HandlerRequest<UpdatingJobConfigRequest, JobMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		final JobID jobId;
		try {
			jobId = request.getPathParameter(JobIDPathParameter.class);
		} catch (Exception e) {
			throw new RestHandlerException(
				"Failed to get JobID parameter.",
				HttpResponseStatus.BAD_REQUEST,	e);
		}

		final  Map<String, String> config;
		List<JobUpdateAction> actions = new ArrayList<>();
		Boolean triggerJobReload = false;
		try {
			config = request.getRequestBody().getConfig();
			Configuration configuration = new Configuration();
			if (config != null && config.size() > 0) {
				log.info("updage job config [{}] jobId [{}]", config.toString(), jobId.toString());
				configuration.addAll(config);
				JobConfigUpdateAction jobConfigUpdateAction = new JobConfigUpdateAction(configuration);
				actions.add(jobConfigUpdateAction);
				if (null != request.getRequestBody().getJobReload()) {
					triggerJobReload = request.getRequestBody().getJobReload();
				}
			}
		} catch (Exception e) {
			throw new RestHandlerException(
				"Failed to deserialize update job config (jobId: " + jobId + ").",
				HttpResponseStatus.BAD_REQUEST, e);
		}

		if (actions.size() > 0) {
			JobUpdateRequest updateRequestrequest = new JobUpdateRequest(actions, triggerJobReload);
			return gateway.updateJob(jobId, updateRequestrequest, timeout)
			.exceptionally(throwable -> {
				log.error("updage job config fail [{}]", jobId.toString());
				throw new CompletionException(new RestHandlerException(
					"Failed to update job (jobId: " + jobId + ").",
					HttpResponseStatus.INTERNAL_SERVER_ERROR,
					ExceptionUtils.stripCompletionException(throwable)));
			})
			.thenApply((Acknowledge response) -> {
				try {
					log.info("updage job config success jobId [{}]", jobId.toString());
					return new JobUpdatingConfigStatus(true);
				} catch (Exception e) {
					log.error("updage job config success jobId [{}] error [{}]", jobId.toString(), e.getMessage());
					throw new CompletionException(new RestHandlerException(
						"Failed to serialize JobUpdatingConfigStatus (jobId: " + jobId + ").",
						HttpResponseStatus.INTERNAL_SERVER_ERROR, e));
				}
			});

		} else {
			return CompletableFuture.completedFuture(new JobUpdatingConfigStatus(false));
		}

	}

}
