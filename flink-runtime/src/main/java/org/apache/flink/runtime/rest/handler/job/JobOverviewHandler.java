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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobOverview;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler returning overview for the specified job.
 */
public class JobOverviewHandler extends AbstractExecutionGraphHandler<JobOverview, JobMessageParameters> implements JsonArchivist {

	private final MetricFetcher<?> metricFetcher;

	public JobOverviewHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, JobOverview, JobMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor,
		MetricFetcher<?> metricFetcher) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);

		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
	}

	@Override
	protected JobOverview handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		return createJobOverview(executionGraph, metricFetcher);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody json = createJobOverview(graph, metricFetcher);
		String path = getMessageHeaders().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singleton(new ArchivedJson(path, json));
	}

	private static JobOverview createJobOverview(AccessExecutionGraph executionGraph, @Nullable MetricFetcher<?> metricFetcher) {
		metricFetcher.update();

		JobID jobId = executionGraph.getJobID();
		MetricStore metricStore = metricFetcher.getMetricStore();
		boolean historyFail = false;
		boolean failover = false;
		Collection<JobOverview.JobVertex> jobInQueueFullVertices = new ArrayList<>();
		Collection<JobOverview.JobVertex> jobOutQueueFullVertices = new ArrayList<>();
		Map<TaskManagerLocation, HashSet<JobOverview.JobVertex>> taskManagerLocation2Vertices = new HashMap<>();
		ErrorInfo rootException = executionGraph.getFailureInfo();
		if (rootException != null) {
			failover = true;
		}
		for (AccessExecutionJobVertex vertex : executionGraph.getVerticesTopologically()) {
			JobVertexID vertexId = vertex.getJobVertexId();
			boolean hasInQueueFullSubTask = false;
			boolean hasOutQueueFullSubTask = false;
			Collection<JobOverview.JobSubtask> subtasks = new ArrayList<>();
			for (AccessExecutionVertex task : vertex.getTaskVertices()) {
				String t = task.getFailureCauseAsString();
				if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
					historyFail = true;
				}
				int subTaskIndex = task.getCurrentExecutionAttempt().getParallelSubtaskIndex();
				MetricStore.ComponentMetricStore subTaskMetric = metricStore.getSubtaskMetricStore(jobId.toString(), vertexId.toString(), subTaskIndex);
				TaskManagerLocation taskManagerLocation = task.getCurrentAssignedResourceLocation();
				ResourceID tmId = taskManagerLocation.getResourceID();
				if (null != subTaskMetric.metrics) {
					if (null != subTaskMetric.metrics.get("buffers.inPoolUsage")) {
						double inPoolUsage = Double.parseDouble(subTaskMetric.metrics.get("buffers.inPoolUsage"));
						if (inPoolUsage > 0.99) {
							hasInQueueFullSubTask = true;
						}
					}
					if (null != subTaskMetric.metrics.get("buffers.outPoolUsage")) {
						double outPoolUsage = Double.parseDouble(subTaskMetric.metrics.get("buffers.outPoolUsage"));
						if (outPoolUsage > 0.99) {
							hasOutQueueFullSubTask = true;
						}
					}
				}
				JobOverview.JobSubtask subtask = new JobOverview.JobSubtask(vertexId, subTaskIndex, tmId, subTaskMetric.metrics);
				subtasks.add(subtask);
				HashSet<JobOverview.JobVertex> jobVertices = taskManagerLocation2Vertices.getOrDefault(taskManagerLocation, new HashSet<>());
				jobVertices.add(new JobOverview.JobVertex(vertexId, vertex.getName(), null));
				taskManagerLocation2Vertices.put(taskManagerLocation, jobVertices);
			}
			JobOverview.JobVertex jobVertex = new JobOverview.JobVertex(vertexId, vertex.getName(), subtasks);
			if (hasInQueueFullSubTask) {
				jobInQueueFullVertices.add(jobVertex);
			} else if (hasOutQueueFullSubTask) {
				jobOutQueueFullVertices.add(jobVertex);
			}
		}
		Collection<JobOverview.JobTaskManager> tms = createTaskManagers(metricStore, taskManagerLocation2Vertices);
		JobOverview jobOverview = new JobOverview(failover, historyFail, jobInQueueFullVertices, jobOutQueueFullVertices, tms);
		return jobOverview;
	}

	private static Collection<JobOverview.JobTaskManager> createTaskManagers(MetricStore metricStore, Map<TaskManagerLocation, HashSet<JobOverview.JobVertex>> taskManagerLocation2Vertices) {
		Collection<JobOverview.JobTaskManager> tms = new ArrayList<>();
		for (Map.Entry<TaskManagerLocation, HashSet<JobOverview.JobVertex>> entry : taskManagerLocation2Vertices.entrySet()) {
			TaskManagerLocation tmLocation = entry.getKey();
			HashSet<JobOverview.JobVertex> vertices = entry.getValue();
			final MetricStore.TaskManagerMetricStore tmMetrics = metricStore.getTaskManagerMetricStore(tmLocation.getResourceID().getResourceIdString());
			JobOverview.JobTaskManager tm = new JobOverview.JobTaskManager(tmLocation.getResourceID(), vertices, tmMetrics.getMetrics());
			tms.add(tm);
		}
		return tms;

	}

}
