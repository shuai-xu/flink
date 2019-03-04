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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Adjust resource and parallelism config for given vertex.
 */
public class AdjustJobConfig implements Action {

	protected JobID jobID;
	protected Map<JobVertexID, Integer> currentParallelism;
	protected Map<JobVertexID, Integer> targetParallelism;
	protected Map<JobVertexID, ResourceSpec> currentResource;
	protected Map<JobVertexID, ResourceSpec> targetResource;
	protected long timeoutMs;
	protected ActionMode actionMode;

	public AdjustJobConfig(JobID jobID, long timeoutMs) {
		this(jobID, timeoutMs, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), ActionMode.IMMEDIATE);
	}

	public AdjustJobConfig(
		JobID jobID,
		long timeoutMs,
		Map<JobVertexID, Integer> currentParallelism,
		Map<JobVertexID, Integer> targetParallelism,
		Map<JobVertexID, ResourceSpec> currentResource,
		Map<JobVertexID, ResourceSpec> targetResource) {
		this(jobID, timeoutMs, currentParallelism, targetParallelism, currentResource, targetResource, ActionMode.IMMEDIATE);
	}

	public AdjustJobConfig(
		JobID jobID,
		long timeoutMs,
		Map<JobVertexID, Integer> currentParallelism,
		Map<JobVertexID, Integer> targetParallelism,
		Map<JobVertexID, ResourceSpec> currentResource,
		Map<JobVertexID, ResourceSpec> targetResource,
		ActionMode actionMode) {
		this.jobID = jobID;
		this.timeoutMs = timeoutMs;
		this.currentParallelism = currentParallelism;
		this.currentResource = currentResource;
		this.targetParallelism = targetParallelism;
		this.targetResource = targetResource;
		this.actionMode = actionMode;
	}

	public void addVertex(
		JobVertexID jobVertexId,
		int currentParallelism,
		int targetParallelism,
		ResourceSpec currentResource,
		ResourceSpec targetResource) {
		this.currentParallelism.put(jobVertexId, currentParallelism);
		this.targetParallelism.put(jobVertexId, targetParallelism);
		this.currentResource.put(jobVertexId, currentResource);
		this.targetResource.put(jobVertexId, targetResource);
	}

	public boolean isEmpty() {
		return currentParallelism.isEmpty();
	}

	@Override
	public void execute(RestServerClient restServerClient) throws Exception {
		Map<JobVertexID, Tuple2<Integer, ResourceSpec>> vertexParallelismResource = new HashMap<>();
		for (JobVertexID jvId : currentParallelism.keySet()) {
			vertexParallelismResource.put(jvId, new Tuple2<>(targetParallelism.get(jvId), targetResource.get(jvId)));
		}
		if (!vertexParallelismResource.isEmpty()) {
			restServerClient.rescale(jobID, vertexParallelismResource);
		}
	}

	@Override
	public boolean validate(MetricProvider provider, RestServerClient restServerClient) throws Exception {
		long start = System.currentTimeMillis();
		while (true) {
			Thread.sleep(timeoutMs / 10);
			RestServerClient.JobStatus jobStatus = restServerClient.getJobStatus(jobID);
			int i = 0;
			for (Tuple2<Long, ExecutionState> time2state: jobStatus.getTaskStatus().values()) {
				if (!time2state.f1.equals(ExecutionState.RUNNING)) {
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
		return new AdjustJobConfig(
				jobID, timeoutMs, targetParallelism, currentParallelism, targetResource, currentResource);
	}

	public void setActionMode(ActionMode actionMode) {
		this.actionMode = actionMode;
	}

	@Override
	public ActionMode getActionMode() {
		return actionMode;
	}

	@Override
	public String toString() {
		String adjustments = currentParallelism.keySet().stream().map(vertexId -> "{JobVertexID:" + vertexId + ", "
			+ "parallelism: " + currentParallelism.get(vertexId) + " -> " + targetParallelism.get(vertexId) + ", "
			+ "resource: " + currentResource.get(vertexId) + " -> " + targetResource.get(vertexId) + "}").collect(
			Collectors.joining(", "));
		return "AdjustJobConfig{actionMode: " + actionMode + ", adjustments: " + adjustments + "}";
	}

	public RestServerClient.JobConfig getAppliedJobConfig(RestServerClient.JobConfig originJobConfig) {
		RestServerClient.JobConfig appliedJobConfig = new RestServerClient.JobConfig(originJobConfig);
		for (JobVertexID vertexId : targetResource.keySet()) {
			RestServerClient.VertexConfig originVertexConfig = originJobConfig.getVertexConfigs().get(vertexId);
			RestServerClient.VertexConfig appliedVertexConfig = new RestServerClient.VertexConfig(
				targetParallelism.get(vertexId),
				originVertexConfig.getMaxParallelism(),
				targetResource.get(vertexId),
				originVertexConfig.getOperatorIds(),
				originVertexConfig.getColocationGroupId());
			appliedJobConfig.getVertexConfigs().put(vertexId, appliedVertexConfig);
		}
		return appliedJobConfig;
	}
}
