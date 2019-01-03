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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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
	 * @param jobID     id of job.
	 * @param startTime start time in millisecond of the range.
	 * @param endTime   end time in millisecond of the range.
	 * @return
	 */
	Map<JobVertexID, List<JobException>> getFailover(JobID jobID, long startTime, long endTime);

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
	 * @param resource    the resource spec of the vertex.
	 */
	void rescale(JobID jobId, JobVertexID vertexId, int parallelism, ResourceSpec resource);

	/**
	 * Configuration of a vertex.
	 */
	class VertexConfig {
		/**
		 * current parallelism.
		 */
		private int parallelism;

		/**
		 * max parallelism.
		 */
		private int maxParallelism;

		/**
		 * current resource request.
		 */
		private ResourceSpec resourceSpec;

		public VertexConfig(int parallelism, int maxParallelism, ResourceSpec resourceSpec) {
			this.parallelism = parallelism;
			this.maxParallelism = maxParallelism;
			this.resourceSpec = resourceSpec;
		}

		public int getParallelism() {
			return parallelism;
		}

		public int getMaxParallelism() {
			return maxParallelism;
		}

		public ResourceSpec getResourceSpec() {
			return resourceSpec;
		}
	}

	/**
	 * Configuration of a job.
	 */
	class JobConfig {
		/**
		 * job level configuration.
		 */
		private Configuration config;

		/**
		 * all vertex configuration.
		 */
		private Map<JobVertexID, VertexConfig> vertexConfigs;

		/**
		 * edges.
		 */
		private Map<JobVertexID, List<JobVertexID>> inputNodes;

		public JobConfig(
				Configuration config,
				Map<JobVertexID, VertexConfig> vertexConfigs,
				Map<JobVertexID, List<JobVertexID>> inputNodes) {
			this.config = config;
			this.vertexConfigs = vertexConfigs;
			this.inputNodes = inputNodes;
		}

		public Configuration getConfig() {
			return config;
		}

		public Map<JobVertexID, VertexConfig> getVertexConfigs() {
			return vertexConfigs;
		}

		public Map<JobVertexID, List<JobVertexID>> getInputNodes() {
			return inputNodes;
		}

	}

	/**
	 * Job status.
	 */
	class JobStatus {

		/**
		 * Status of all task.
		 */
		private Map<ExecutionVertexID, ExecutionState> taskStatus;

		public JobStatus(Map<ExecutionVertexID, ExecutionState> taskStatus) {
			this.taskStatus = taskStatus;
		}

		public Map<ExecutionVertexID, ExecutionState> getTaskStatus() {
			return taskStatus;
		}
	}

}
