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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Adjust resource and parallelism config for given vertex.
 */
public class AdjustJobConfig implements Action {

	private static final Logger LOGGER = LoggerFactory.getLogger(AdjustJobConfig.class);

	protected JobID jobID;
	protected Map<JobVertexID, Integer> currentParallelism;
	protected Map<JobVertexID, Integer> targetParallelism;
	protected Map<JobVertexID, ResourceSpec> currentResource;
	protected Map<JobVertexID, ResourceSpec> targetResource;
	protected long timeoutMs;
	protected ActionMode actionMode;
	private Exception rescaleException = null;

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

	public void exculdeMinorDiffVertices(Configuration conf) {
		double minDiffParallelismRatio = conf.getDouble(HealthMonitorOptions.PARALLELISM_SCALE_MIN_DIFF_RATIO);
		double minDiffResourceRatio = conf.getDouble(HealthMonitorOptions.RESOURCE_SCALE_MIN_DIFF_RATIO);
		double minDiffCpuCore = conf.getDouble(HealthMonitorOptions.RESOURCE_SCALE_MIN_DIFF_CPU);
		int minDiffNativeMemMB = conf.getInteger(HealthMonitorOptions.RESOURCE_SCALE_MIN_DIFF_NATIVE_MEM);

		HashSet<JobVertexID> vertexToRemove = new HashSet<>();
		for (JobVertexID vertexID : currentParallelism.keySet()) {
			int curPara = currentParallelism.get(vertexID);
			int tarPara = targetParallelism.get(vertexID);
			ResourceSpec curRes = currentResource.get(vertexID);
			ResourceSpec tarRes = targetResource.get(vertexID);

			if (Math.abs(curPara - tarPara) > minDiffParallelismRatio * curPara) {
				continue;
			}

			if (Math.abs(curRes.getCpuCores() - tarRes.getCpuCores()) > minDiffResourceRatio * curRes.getCpuCores() &&
				Math.abs(curRes.getCpuCores() - tarRes.getCpuCores()) > minDiffCpuCore) {
				continue;
			}

			if (Math.abs(curRes.getHeapMemory() - tarRes.getHeapMemory()) > minDiffResourceRatio * curRes.getHeapMemory()) {
				continue;
			}

			if (Math.abs(curRes.getDirectMemory() - tarRes.getDirectMemory()) > minDiffResourceRatio * curRes.getDirectMemory()) {
				continue;
			}

			if (Math.abs(curRes.getNativeMemory() - tarRes.getNativeMemory()) > minDiffResourceRatio * curRes.getNativeMemory() &&
				Math.abs(curRes.getNativeMemory() - tarRes.getNativeMemory()) > minDiffNativeMemMB) {
				continue;
			}

			vertexToRemove.add(vertexID);
		}

		for (JobVertexID vertexID : vertexToRemove) {
			LOGGER.debug("Removing vertex with minor difference, vertex id: {}", vertexID);
			currentParallelism.remove(vertexID);
			targetParallelism.remove(vertexID);
			currentResource.remove(vertexID);
			targetResource.remove(vertexID);
		}
	}

	@Override
	public void execute(RestServerClient restServerClient) throws Exception {
		Map<JobVertexID, Tuple2<Integer, ResourceSpec>> vertexParallelismResource = new HashMap<>();
		for (JobVertexID jvId : currentParallelism.keySet()) {
			vertexParallelismResource.put(jvId, new Tuple2<>(targetParallelism.get(jvId), targetResource.get(jvId)));
		}
		if (!vertexParallelismResource.isEmpty()) {
			CompletableFuture<Acknowledge> rescaleFuture = restServerClient.rescale(jobID, vertexParallelismResource);
			rescaleFuture.whenComplete((ignored, throwable) -> {
				if (throwable != null) {
					rescaleException = new Exception("Execute action failed.", throwable);
				}
			});
		}
	}

	@Override
	public boolean validate(MetricProvider provider, RestServerClient restServerClient) throws Exception {
		long start = System.currentTimeMillis();
		while (true) {
			Thread.sleep(timeoutMs / 10);
			if (rescaleException != null) {
				LOGGER.error("Action {} execute failed because: ", this.toString(), rescaleException.getMessage());
				return false;
			}
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
