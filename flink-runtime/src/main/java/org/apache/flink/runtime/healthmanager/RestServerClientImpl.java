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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobGraphOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobGraphOverviewInfo;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobAllSubtaskCurrentAttemptsInfoHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubtaskCurrentAttemptsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentsMetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.JobTaskManagersComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobTaskManagersComponentMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsMessageParameters;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

	public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(
		M messageHeaders,
		U messageParameters,
		R request) throws IOException {
		return restClient.sendRequest(baseUri.getHost(), baseUri.getPort(), messageHeaders,
			messageParameters, request, Collections.emptyList());
	}

	@Override
	public List<JobStatusMessage> listJob() throws Exception {
		return sendRequest(
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

	//依赖 jobGraph， 可以先用伪接口实现
	@Override
	public JobConfig getJobConfig(JobID jobId) {
		final JobGraphOverviewHeaders header = JobGraphOverviewHeaders.getInstance();
		final JobMessageParameters parameters = header.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobId);
		try {
			return sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(JobGraphOverviewInfo jobGraphOverviewInfo) -> {
					Map<JobVertexID, VertexConfig> vertexConfigs = new HashMap<>();
					Map<JobVertexID, List<JobVertexID>> inputNodes = jobGraphOverviewInfo.getInputNodes();
					for (Map.Entry<JobVertexID, JobGraphOverviewInfo.VertexConfigInfo> vertexId2Config: jobGraphOverviewInfo.getVertexConfigs().entrySet()) {
						JobGraphOverviewInfo.VertexConfigInfo jobGraphVertexConfig = vertexId2Config.getValue();
						VertexConfig vertexConfig = new VertexConfig(jobGraphVertexConfig.getParallelism(), jobGraphVertexConfig.getMaxParallelism(),
							jobGraphVertexConfig.getResourceSpec(), jobGraphVertexConfig.getOperatorIds());
						vertexConfigs.put(vertexId2Config.getKey(), vertexConfig);
					}
					return new JobConfig(jobGraphOverviewInfo.getConfig(), vertexConfigs, inputNodes);
				}
			).get();
		} catch (Exception ignore) {
			return null;
		}
	}

	//JobAllSubtaskCurrentAttemptsHandler
	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		final JobAllSubtaskCurrentAttemptsInfoHeaders jobAllSubtaskCurrentAttemptsInfoHeaders =
			JobAllSubtaskCurrentAttemptsInfoHeaders.getInstance();
		final JobMessageParameters jobMessageParameters = jobAllSubtaskCurrentAttemptsInfoHeaders
			.getUnresolvedMessageParameters();
		jobMessageParameters.jobPathParameter.resolve(jobId);
		return sendRequest(jobAllSubtaskCurrentAttemptsInfoHeaders, jobMessageParameters, EmptyRequestBody.getInstance()).thenApply(
			(JobSubtaskCurrentAttemptsInfo subtasksInfo) -> {
				Collection<SubtaskExecutionAttemptInfo> subtasks = subtasksInfo.getSubtaskInfos();
				Map<ExecutionVertexID, ExecutionState> taskStatus = new HashMap<>();
				for (SubtaskExecutionAttemptInfo subtask: subtasks) {
					JobVertexID jobVertexID = JobVertexID.fromHexString(subtask.getVertexId());
					ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, subtask.getSubtaskIndex());
					ExecutionState executionState = subtask.getStatus();
					taskStatus.put(executionVertexID, executionState);
				}
				return new JobStatus(taskStatus);
			}
		).get();
	}

	//@ JobExceptionsHandler
	// org.apache.flink.client.program.rest.RestClusterClient.rescaleJob
	@Override
	public Map<JobVertexID, List<JobException>> getFailover(JobID jobID, long startTime, long endTime) throws Exception {
		final JobExceptionsHeaders jobExceptionsHeaders = JobExceptionsHeaders.getInstance();
		final JobMessageParameters jobMessageParameters = jobExceptionsHeaders.getUnresolvedMessageParameters();
		jobMessageParameters.jobPathParameter.resolve(jobID);
		return sendRequest(jobExceptionsHeaders, jobMessageParameters, EmptyRequestBody.getInstance()).thenApply(
			(JobExceptionsInfo exceptionsInfo) -> {
				List<JobExceptionsInfo.ExecutionExceptionInfo> exceptions = exceptionsInfo.getAllExceptions();
				Map<JobVertexID, List<JobException>> jobVertexId2exceptions = new HashMap<>();
				for (JobExceptionsInfo.ExecutionExceptionInfo exception : exceptions) {
					JobVertexID jobVertexID = JobVertexID.fromHexString(exception.getVertexID());
					if (exception.getTimestamp() >= startTime && exception.getTimestamp() <= endTime) {
						JobException vertexException = new JobException(exception.getException());
						List<JobException> vertexExceptions;
						if (jobVertexId2exceptions.containsKey(jobVertexID)) {
							vertexExceptions = jobVertexId2exceptions.get(jobVertexID);
						} else {
							vertexExceptions = new ArrayList<>();
						}
						vertexExceptions.add(vertexException);
						jobVertexId2exceptions.put(jobVertexID, vertexExceptions);
					} else {
						continue;
					}
				}
				return jobVertexId2exceptions;
			}
		).get();
	}

	@Override
	public List<ExecutionVertexID> getTaskManagerTasks(String tmId) {
		return null;
	}

	//需要获取 vertex 的所有 metrics
	//@return metric values in a map: [metric name, [subtask index, [fetch timestamp, metric value]]]
	//todo: how to handle exception
	@Override
	public Map<String, Map<Integer, Tuple2<Long, Double>>> getTaskMetrics(JobID jobID, JobVertexID jobVertexID,
																		Set<String> metricNames) {
		final JobVertexSubtasksComponentMetricsHeaders header = JobVertexSubtasksComponentMetricsHeaders.getInstance();
		final JobVertexSubtasksComponentMetricsMessageParameters parameters = header.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobID);
		parameters.jobVertexIdPathParameter.resolve(jobVertexID);
		List<String> metricNameList = new ArrayList<>();
		metricNameList.addAll(metricNames);
		parameters.metricsFilterParameter.resolve(metricNameList);
		Map<String, Map<Integer, Tuple2<Long, Double>>> result = new HashMap<>();
		try {
			return sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(ComponentsMetricCollectionResponseBody cmc) -> {
					Collection<ComponentMetric> componentMetrics = cmc.getComponentMetrics();
					for (ComponentMetric componentMetric: componentMetrics) {
						Integer componentId = Integer.valueOf(componentMetric.getComponentId());
						Long timestamp = componentMetric.getTimestamp();
						for (Metric metric: componentMetric.getMetrics()) {
							String metricName = metric.getId();
							Double metricValue = Double.valueOf(metric.getValue());
							Map<Integer, Tuple2<Long, Double>> metricMap = result.get(metric.getId());
							if (metricMap == null) {
								metricMap = new HashMap<>(componentMetrics.size());
							}
							metricMap.put(componentId, Tuple2.of(timestamp, metricValue));
							result.put(metricName, metricMap);
						}
					}
					return result;
				}
			).get();
		} catch (Exception ignore) {
		}
		return result;
	}

	//获取所有 tm 的 metrics
	@Override
	public Map<String, Map<String, Tuple2<Long, Double>>> getTaskManagerMetrics(Set<String> tmIds,
			Set<String> metricNames) {
		final TaskManagersComponentMetricsHeaders header = TaskManagersComponentMetricsHeaders.getInstance();
		final TaskManagersComponentMetricsMessageParameters parameters = header.getUnresolvedMessageParameters();
		List<String> metricNameList = new ArrayList<>();
		metricNameList.addAll(metricNames);
		parameters.metricsFilterParameter.resolve(metricNameList);
		Map<String, Map<String, Tuple2<Long, Double>>> result = new HashMap<>();
		try {
			return sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(ComponentsMetricCollectionResponseBody cmc) -> {
					return updateMetricFromComponentsMetricCollection(cmc, result);
				}
			).get();
		} catch (Exception ignore) {
		}
		return result;
	}

	//获取
	@Override
	public Map<String, Map<String, Tuple2<Long, Double>>> getTaskManagerMetrics(JobID jobId,
			Set<String> metricNames) {
		final JobTaskManagersComponentMetricsHeaders header = JobTaskManagersComponentMetricsHeaders.getInstance();
		final JobTaskManagersComponentMetricsMessageParameters parameters = header.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobId);
		List<String> metricNameList = new ArrayList<>();
		metricNameList.addAll(metricNames);
		parameters.metricsFilterParameter.resolve(metricNameList);
		Map<String, Map<String, Tuple2<Long, Double>>> result = new HashMap<>();
		try {
			return sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(ComponentsMetricCollectionResponseBody cmc) -> {
					return updateMetricFromComponentsMetricCollection(cmc, result);
				}
			).get();
		} catch (Exception ignore) {
		}
		return result;
	}

	@Override
	public void rescale(JobID jobId, Map<JobVertexID, Tuple2<Integer, ResourceSpec>> vertexParallelismResource) {

	}

	private Map<String, Map<String, Tuple2<Long, Double>>> updateMetricFromComponentsMetricCollection(ComponentsMetricCollectionResponseBody cmc,
																									Map<String, Map<String, Tuple2<Long, Double>>> result){
		Collection<ComponentMetric> componentMetrics = cmc.getComponentMetrics();
		for (ComponentMetric componentMetric: componentMetrics) {
			String componentId = componentMetric.getComponentId();
			Long timestamp = componentMetric.getTimestamp();
			for (Metric metric: componentMetric.getMetrics()) {
				String metricName = metric.getId();
				Double metricValue = Double.valueOf(metric.getValue());
				Map<String, Tuple2<Long, Double>> metricMap = result.get(metric.getId());
				if (metricMap == null) {
					metricMap = new HashMap<>(componentMetrics.size());
				}
				metricMap.put(componentId, Tuple2.of(timestamp, metricValue));
				result.put(metricName, metricMap);
			}
		}
		return result;
	}
}
