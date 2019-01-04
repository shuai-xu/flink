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

package org.apache.flink.runtime.healthmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Implementation of java sdk of rest server.
 */
public class RestServerClientImpl implements RestServerClient {

	private RestClient restClient;
	private URI baseUri;

	public RestServerClientImpl(
			String baseUrl, Configuration config, Executor executor) throws Exception {

		RestClientConfiguration restClientConfiguration =
				RestClientConfiguration.fromConfiguration(config);
		restClient = new RestClient(restClientConfiguration, executor);
		baseUri = new URI(baseUrl);
	}

	@Override
	public List<JobStatusMessage> listJob() throws Exception {
		return restClient.sendRequest(
				baseUri.getHost(),
				baseUri.getPort(),
				JobsOverviewHeaders.getInstance(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance()).thenApply(
						(multipleJobsDetails) -> multipleJobsDetails
								.getJobs()
								.stream()
								.map(detail -> new JobStatusMessage(
										detail.getJobId(),
										detail.getJobName(),
										detail.getStatus(),
										detail.getStartTime()))
								.collect(Collectors.toList())).get();
	}

	@Override
	public JobConfig getJobConfig(JobID jobId) {
		return null;
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) {
		return null;
	}

	@Override
	public Map<JobVertexID, List<JobException>> getFailover(JobID jobID, long startTime, long endTime) {
		return null;
	}

	@Override
	public Map<String, Map<Integer, Tuple2<Long, Double>>> getTaskMetrics(JobID jobID, JobVertexID jobVertexID,
			Set<String> metricNames) {
		return null;
	}

	@Override
	public Map<String, Map<String, Tuple2<Long, Double>>> getTaskManagerMetrics(Set<String> tmIds,
			Set<String> metricNames) {
		return null;
	}

	@Override
	public Map<String, Map<String, Tuple2<Long, Double>>> getTaskManagerMetrics(JobID jobId,
			Set<String> metricNames) {
		return null;
	}

	@Override
	public void rescale(JobID jobId, JobVertexID vertexId, int parallelism, ResourceSpec resourceSpec) {

	}
}
