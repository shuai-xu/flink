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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.UpdatingJobRequest;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for triggering the updating of a job.
 */
public class UpdatingTriggerHeaders extends
	AsynchronousOperationTriggerMessageHeaders<UpdatingJobRequest, JobMessageParameters> {

	private static final UpdatingTriggerHeaders INSTANCE = new UpdatingTriggerHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/updating",
		JobIDPathParameter.KEY);

	private UpdatingTriggerHeaders() {}

	@Override
	public Class<UpdatingJobRequest> getRequestClass() {
		return UpdatingJobRequest.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public JobMessageParameters getUnresolvedMessageParameters() {
		return new JobMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.PATCH;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static UpdatingTriggerHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Triggers the updating of a job.";
	}
}
