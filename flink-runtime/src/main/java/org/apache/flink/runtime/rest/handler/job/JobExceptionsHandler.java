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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsEndFilterQueryParameter;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsStartFilterQueryParameter;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.util.FixedSortedSet;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler serving the job exceptions.
 */
public class JobExceptionsHandler extends AbstractExecutionGraphsHandler<JobExceptionsInfo, JobExceptionsMessageParameters> implements JsonArchivist {

	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 200;

	public JobExceptionsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobExceptionsInfo, JobExceptionsMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {

		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobExceptionsInfo handleRequest(HandlerRequest<EmptyRequestBody, JobExceptionsMessageParameters> request,
		AccessExecutionGraph executionGraph, EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs) {
		List<Long> startList = request.getQueryParameter(JobExceptionsStartFilterQueryParameter.class);
		List<Long> endList = request.getQueryParameter(JobExceptionsEndFilterQueryParameter.class);
		Long start = startList.isEmpty() ? -1L : startList.get(0);
		Long end = endList.isEmpty() ? System.currentTimeMillis() : endList.get(0);
		return createJobExceptionsInfo(executionGraph, historicalGraphs, start, end);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody json = createJobExceptionsInfo(graph, null, -1L, System.currentTimeMillis());
		String path = getMessageHeaders().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singletonList(new ArchivedJson(path, json));
	}

	private static JobExceptionsInfo createJobExceptionsInfo(AccessExecutionGraph executionGraph,
		EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs, Long start, Long end) {
		ErrorInfo rootException = executionGraph.getFailureInfo();
		String rootExceptionMessage = null;
		Long rootTimestamp = null;
		if (rootException != null) {
			rootExceptionMessage = rootException.getExceptionAsString();
			rootTimestamp = rootException.getTimestamp();
		}

		// TODO: Traversing the execution graph costs too much, rewrite it with other ways.
		// Use DESC ordering, since we want to keep most recent exceptions.
		final FixedSortedSet<JobExceptionsInfo.ExecutionExceptionInfo>
			taskExceptionList = new FixedSortedSet<>(MAX_NUMBER_EXCEPTION_TO_REPORT, Comparator.reverseOrder());
		int numExceptionsSofar = 0;
		boolean truncated = false;
		for (AccessExecutionJobVertex currentJobVertex : executionGraph.getVerticesTopologically()) {
			List<AccessExecutionJobVertex> vertexList = getVertex(historicalGraphs, currentJobVertex);
			for (AccessExecutionJobVertex jobVertex : vertexList) {
				for (AccessExecutionVertex executionVertex : jobVertex.getTaskVertices()) {
					// Task can not be null.
					AccessExecution task = executionVertex.getCurrentExecutionAttempt();
					JobExceptionsInfo.ExecutionExceptionInfo
						executionExceptionInfo = generateExecutionExceptionInfo(
						jobVertex,
						executionVertex,
						task);
					if (executionExceptionInfo != null && executionExceptionInfo.getTimestamp() >= start
						&& executionExceptionInfo.getTimestamp() <= end) {
						taskExceptionList.add(executionExceptionInfo);
						numExceptionsSofar++;
					}

					for (int i = task.getAttemptNumber() - 1; i >= 0; i--) {
						try {
							task = executionVertex.getPriorExecutionAttempt(i);
						} catch (Exception e) {
							break;
						}
						executionExceptionInfo = generateExecutionExceptionInfo(
							jobVertex,
							executionVertex,
							task);
						if (executionExceptionInfo != null && executionExceptionInfo.getTimestamp() >= start
							&& executionExceptionInfo.getTimestamp() <= end) {
							taskExceptionList.add(executionExceptionInfo);
							numExceptionsSofar++;
						}
					}
					if (!truncated && numExceptionsSofar >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
						truncated = true;
					}
				}
			}
		}

		return new JobExceptionsInfo(rootExceptionMessage, rootTimestamp, new ArrayList<>(taskExceptionList), truncated);
	}

	private static List<AccessExecutionJobVertex> getVertex(EvictingBoundedList<ArchivedExecutionGraph> historicalGraphs, AccessExecutionJobVertex jobVertex){
		List<AccessExecutionJobVertex> vertexList = new ArrayList<>();
		vertexList.add(jobVertex);
		if (historicalGraphs != null) {
			for (ArchivedExecutionGraph graph: historicalGraphs) {
				AccessExecutionJobVertex v = graph.getJobVertex(jobVertex.getJobVertexId());
				vertexList.add(v);
			}
		}
		return vertexList;
	}

	private static JobExceptionsInfo.ExecutionExceptionInfo generateExecutionExceptionInfo(AccessExecutionJobVertex jobVertex, AccessExecutionVertex executionVertex, AccessExecution task) {
		String t = task != null ? task.getFailureCauseAsString() : null;
		if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
			TaskManagerLocation location = task.getAssignedResourceLocation();
			String locationString = location != null ?
				location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";
			long timestamp = task.getStateTimestamp(ExecutionState.FAILED);

			return new JobExceptionsInfo.ExecutionExceptionInfo(
				t,
				executionVertex.getTaskNameWithSubtaskIndex(),
				locationString,
				timestamp == 0 ? -1 : timestamp,
				jobVertex.getJobVertexId().toString(),
				task.getParallelSubtaskIndex(),
				task.getAttemptNumber());
		} else {
			return null;
		}
	}
}
