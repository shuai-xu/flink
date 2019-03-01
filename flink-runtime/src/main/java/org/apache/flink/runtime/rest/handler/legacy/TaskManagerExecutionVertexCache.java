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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.ExecutionVertexIDInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Cache for taskmanager list {@link ExecutionVertexIDInfo} which are obtained from the Flink cluster. Every cache entry
 * has an associated time to live after which a new request will trigger the reloading of it.
 */
public class TaskManagerExecutionVertexCache implements Closeable {

	private final Time timeout;

	private final Time timeToLive;

	private final Map<ResourceID, List<ExecutionVertexIDInfo>> cachedExcutionVertixIds;

	private volatile boolean running = true;

	private long ttl;

	public TaskManagerExecutionVertexCache(
			Time timeout,
			Time timeToLive) {
		this.timeout = checkNotNull(timeout);
		this.timeToLive = checkNotNull(timeToLive);
		this.ttl = System.currentTimeMillis() + timeToLive.toMilliseconds();

		cachedExcutionVertixIds = new ConcurrentHashMap<>(100);
	}

	@Override
	public void close() {
		running = false;

		cachedExcutionVertixIds.clear();
	}

	/**
	 * Gets the number of cache entries.
	 */
	public int size() {
		return cachedExcutionVertixIds.size();
	}

	/**
	 * Gets the List ExecutionVertexID for the given taskmanagerId and caches it.
	 */
	public List<ExecutionVertexIDInfo> getTaskManagerExecutionVertex(ResourceID taskmanagerId, RestfulGateway restfulGateway, ExecutionGraphCache executionGraphCache) throws Exception {

		Preconditions.checkState(running, "TaskManagerExecutionVertexCache is no longer running");

		List<ExecutionVertexIDInfo> executionVertexIds = cachedExcutionVertixIds.get(taskmanagerId);

		long currentTime = System.currentTimeMillis();

		if (executionVertexIds != null && currentTime < ttl) {
			return executionVertexIds;
		} else {
			MultipleJobsDetails jobs = restfulGateway.requestMultipleJobDetails(timeout).get();
			Map<ResourceID, List<ExecutionVertexIDInfo>> tmId2ExcutionVertixIds = new HashMap<>();
			if (jobs != null && jobs.getJobs().size() > 0) {
				List<JobID> jobIds = jobs.getJobs().stream().filter(job -> job.getStatus() == JobStatus.RUNNING)
					.map(JobDetails::getJobId).collect(Collectors.toList());
				for (JobID jobid: jobIds) {
					AccessExecutionGraph executionGraph = executionGraphCache.getExecutionGraph(jobid, restfulGateway).get();
					for (AccessExecutionJobVertex accessExecutionJobVertex : executionGraph.getVerticesTopologically()) {
						JobVertexID jobVertexID = accessExecutionJobVertex.getJobVertexId();
						for (AccessExecutionVertex executionVertex : accessExecutionJobVertex.getTaskVertices()) {
							final AccessExecution execution = executionVertex.getCurrentExecutionAttempt();
							if (null != execution.getAssignedResourceLocation()) {
								final ResourceID currentTaskTaskmanagerId = execution.getAssignedResourceLocation().getResourceID();
								List<ExecutionVertexIDInfo> tmExecutionVertexIds = tmId2ExcutionVertixIds.get(currentTaskTaskmanagerId);
								if (tmExecutionVertexIds == null) {
									tmExecutionVertexIds = new ArrayList<>();
								}
								ExecutionVertexIDInfo executionVertexId = new ExecutionVertexIDInfo(jobVertexID, execution.getParallelSubtaskIndex());
								tmExecutionVertexIds.add(executionVertexId);
								tmId2ExcutionVertixIds.put(currentTaskTaskmanagerId, tmExecutionVertexIds);
							}
						}
					}
				}
			}
			cachedExcutionVertixIds.putAll(tmId2ExcutionVertixIds);
			this.ttl = currentTime + timeToLive.toMilliseconds();
			return cachedExcutionVertixIds.get(taskmanagerId);
		}
	}

}
