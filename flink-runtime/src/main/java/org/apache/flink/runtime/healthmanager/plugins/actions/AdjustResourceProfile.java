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

package org.apache.flink.runtime.healthmanager.plugins.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.MetricProvider;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Adjust resource profile for given vertex.
 */
public class AdjustResourceProfile implements Action {

	private JobID jobID;
	private JobVertexID vertexID;
	private int currentParallelism;
	private int targetParallelism;
	private ResourceSpec currentResource;
	private ResourceSpec targetResource;
	private long timeoutMs;

	public AdjustResourceProfile(
			JobID jobID,
			JobVertexID vertexID,
			int currentParallelism,
			int targetParallelism,
			ResourceSpec currentResource,
			ResourceSpec targetResource,
			long timeoutMs) {
		this.jobID = jobID;
		this.vertexID = vertexID;
		this.currentParallelism = currentParallelism;
		this.currentResource = currentResource;
		this.targetParallelism = targetParallelism;
		this.targetResource = targetResource;
		this.timeoutMs = timeoutMs;
	}

	@Override
	public void execute(RestServerClient restServerClient) throws InterruptedException {
		restServerClient.rescale(jobID, vertexID, targetParallelism, targetResource);
	}

	@Override
	public boolean validate(MetricProvider provider, RestServerClient restServerClient) throws InterruptedException {
		long start = System.currentTimeMillis();
		while (true) {
			Thread.sleep(timeoutMs / 10);
			RestServerClient.JobStatus jobStatus = restServerClient.getJobStatus(jobID);
			int i = 0;
			for (ExecutionState state: jobStatus.getTaskStatus().values()) {
				if (!state.equals(ExecutionState.RUNNING)) {
					break;
				}
				i++;
			}

			// all task running now.
			if (i == jobStatus.getTaskStatus().size()) {
				break;
			}

			if (System.currentTimeMillis() - start > timeoutMs) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Action rollback() {
		return new AdjustResourceProfile(
				jobID, vertexID, targetParallelism, currentParallelism, targetResource, currentResource, timeoutMs);
	}
}
