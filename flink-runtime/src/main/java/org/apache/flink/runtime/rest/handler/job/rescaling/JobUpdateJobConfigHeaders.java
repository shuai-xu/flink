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
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobUpdatingConfigStatus;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.UpdatingJobConfigRequest;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for polling the status of an ongoing rescaling operation.
 */
public class JobUpdateJobConfigHeaders implements MessageHeaders<UpdatingJobConfigRequest, JobUpdatingConfigStatus, JobMessageParameters> {

	private static final JobUpdateJobConfigHeaders INSTANCE = new JobUpdateJobConfigHeaders();

	private static final String URL = String.format(
		"/jobs/:%s/updating/config",
		JobIDPathParameter.KEY);

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static JobUpdateJobConfigHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public Class<JobUpdatingConfigStatus> getResponseClass() {
		return JobUpdatingConfigStatus.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<UpdatingJobConfigRequest> getRequestClass() {
		return UpdatingJobConfigRequest.class;
	}

	@Override
	public JobMessageParameters getUnresolvedMessageParameters() {
		return new JobMessageParameters();
	}

	@Override
	public String getDescription() {
		return "Update the config of job.";
	}

}
