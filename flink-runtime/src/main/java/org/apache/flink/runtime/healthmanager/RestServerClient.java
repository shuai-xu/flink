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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Java SDK of rest server.
 */
public interface RestServerClient {

	/**
	 * List all job running.
	 * @return
	 */
	List<JobStatusMessage> listJob() throws Exception;

	/**
	 * Get job config.
	 * @param jobId job id of the job.
	 * @return
	 */
	JobConfig getJobConfig(JobID jobId);

	/**
	 * Get job status.
	 * @param jobId job id of the job.
	 * @return
	 */
	JobStatus getJobStatus(JobID jobId);

	/**
	 * Get failover history in time range.
	 * @param startTime start time of the range.
	 * @param endTime   end time of the range.
	 * @return
	 */
	List<JobException> getFailover(Timestamp startTime, Timestamp endTime);

	/**
	 * Get Metrics of given job vertex.
	 * @param jobID        job id of the job.
	 * @param jobVertexID  job vertex id of the vertex.
	 * @param metricNames  metric names to retrieve.
	 * @return metric values.
	 */
	Map<String, Map<Integer, Double>> getMetrics(JobID jobID, JobVertexID jobVertexID, Set<String> metricNames);

	/**
	 * Rescale given job vertex.
	 * @param jobId       job id of the job.
	 * @param vertexId    job vertex id of the vertex.
	 * @param parallelism new parallelism of the job.
	 */
	void rescale(JobID jobId, JobVertexID vertexId, int parallelism);

	/**
	 * Configuration of a vertex.
	 */
	class VertexConfig {
		int parallelism;
		int maxParallelism;
		ResourceProfile resourceProfile;
	}

	/**
	 * Configuration of a job.
	 */
	class JobConfig {
		Configuration config;
		Map<JobVertexID, VertexConfig> vertexConfigs;
		Map<JobVertexID, List<JobVertexID>> inputNodes;
	}

	/**
	 * Job status.
	 */
	class JobStatus {
		JobStatus jobStatus;
		Map<ExecutionVertexID, ExecutionState> taskStatus;
	}

}
