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
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.handler.job.rescaling.UpdatingTriggerHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ExecutionVertexIDInfo;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobGraphOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobGraphOverviewInfo;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResourceSpecInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TotalResourceLimitExceptionInfosHeaders;
import org.apache.flink.runtime.rest.messages.TotalResourceLimitExceptionsInfos;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatisticDetailsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatisticsHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.TaskCheckpointStatistics;
import org.apache.flink.runtime.rest.messages.job.JobAllSubtaskCurrentAttemptsInfoHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubtaskCurrentAttemptsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptInfo;
import org.apache.flink.runtime.rest.messages.job.UpdatingJobRequest;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.ComponentsMetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.JobTaskManagersComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobTaskManagersComponentMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexSubtasksComponentMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagersComponentMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExceptionsInfos;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskmanagerAllSubtaskCurrentAttemptsInfoHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskmanagersAllSubtaskCurrentAttemptsInfoHeaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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

	private static final Logger LOGGER = LoggerFactory.getLogger(RestServerClientImpl.class);

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
						(multipleJobsDetails) -> {
							if (multipleJobsDetails != null && multipleJobsDetails.getJobs().size() > 0){
								return multipleJobsDetails
										.getJobs()
										.stream()
										.map(detail -> new JobStatusMessage(
											detail.getJobId(),
											detail.getJobName(),
											detail.getStatus(),
											detail.getStartTime()))
										.collect(Collectors.toList());
							} else {
								return new ArrayList<JobStatusMessage>();
							}
						}).get();
	}

	@Override
	public JobConfig getJobConfig(JobID jobId) {
		final JobGraphOverviewHeaders header = JobGraphOverviewHeaders.getInstance();
		JobMessageParameters parameters = header.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobId);
		parameters.isResolved();
		try {
			return sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				jobGraphOverviewInfo -> {
					Map<JobVertexID, VertexConfig> vertexConfigs = new HashMap<>();
					Map<JobVertexID, List<Tuple2<JobVertexID, String>>> inputNodes = new HashMap<>();
					for (Map.Entry<String, JobGraphOverviewInfo.VertexConfigInfo> vertexId2Config: jobGraphOverviewInfo.getVertexConfigs().entrySet()) {
						JobGraphOverviewInfo.VertexConfigInfo jobGraphVertexConfig = vertexId2Config.getValue();
						JobVertexID vertexID = JobVertexID.fromHexString(vertexId2Config.getKey());
						VertexConfig vertexConfig = new VertexConfig(jobGraphVertexConfig.getParallelism(), jobGraphVertexConfig.getMaxParallelism(),
							jobGraphVertexConfig.getResourceSpec().convertToResourceSpec(), jobGraphVertexConfig.getNodeIds(),
							jobGraphVertexConfig.getCoLocationGroupId());
						vertexConfigs.put(vertexID, vertexConfig);
						List<Tuple2<JobVertexID, String>> inputVertexIds = jobGraphOverviewInfo.getInputNodes().get(vertexId2Config.getKey()).stream().map(vertexId2ship ->
							Tuple2.of(JobVertexID.fromHexString(vertexId2ship.getInputVertexId()), vertexId2ship.getShipStrategyName())).collect(Collectors.toList());
						inputNodes.put(vertexID, inputVertexIds);
					}
					Configuration configuration = new Configuration();
					if (jobGraphOverviewInfo.getConfig() != null) {
						configuration.addAll(jobGraphOverviewInfo.getConfig());
					}
					return new JobConfig(configuration, vertexConfigs, inputNodes);
				}
			).get();
		} catch (Exception ignore) {
			return null;
		}
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		final JobAllSubtaskCurrentAttemptsInfoHeaders headers =
			JobAllSubtaskCurrentAttemptsInfoHeaders.getInstance();
		JobMessageParameters parameters = headers.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobId);
		return sendRequest(headers, parameters, EmptyRequestBody.getInstance()).thenApply(
			(JobSubtaskCurrentAttemptsInfo subtasksInfo) -> {
				Collection<SubtaskExecutionAttemptInfo> subtasks = subtasksInfo.getSubtaskInfos();
				Map<ExecutionVertexID, Tuple2<Long, ExecutionState>> taskStatus = new HashMap<>();
				for (SubtaskExecutionAttemptInfo subtask: subtasks) {
					JobVertexID jobVertexID = JobVertexID.fromHexString(subtask.getVertexId());
					ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, subtask.getSubtaskIndex());
					taskStatus.put(executionVertexID, Tuple2.of(subtask.getCurrentStateTime(), subtask.getStatus()));
				}
				return new JobStatus(taskStatus);
			}
		).get();
	}

	@Override
	public Map<JobVertexID, List<JobException>> getFailover(JobID jobID, long startTime, long endTime) throws Exception {
		final JobExceptionsHeaders headers = JobExceptionsHeaders.getInstance();
		final JobExceptionsMessageParameters parameters = headers.getUnresolvedMessageParameters();
		parameters.jobPathParameter.resolve(jobID);
		List<Long> startList = new ArrayList<>();
		startList.add(startTime);
		List<Long> endList = new ArrayList<>();
		endList.add(endTime);
		parameters.start.resolve(startList);
		parameters.end.resolve(endList);
		Map<JobVertexID, List<JobException>> failovers = sendRequest(headers, parameters, EmptyRequestBody.getInstance())
			.thenApply((JobExceptionsInfo exceptionsInfo) -> {
				List<JobExceptionsInfo.ExecutionExceptionInfo> exceptions = exceptionsInfo.getAllExceptions();
				Map<JobVertexID, List<JobException>> jobVertexId2exceptions = new HashMap<>();
				for (JobExceptionsInfo.ExecutionExceptionInfo exception : exceptions) {
					JobVertexID jobVertexID = JobVertexID.fromHexString(exception.getVertexID());
					JobException vertexException = new JobException(exception.getException());
					List<JobException> vertexExceptions;
					if (jobVertexId2exceptions.containsKey(jobVertexID)) {
						vertexExceptions = jobVertexId2exceptions.get(jobVertexID);
					} else {
						vertexExceptions = new ArrayList<>();
					}
					vertexExceptions.add(vertexException);
					jobVertexId2exceptions.put(jobVertexID, vertexExceptions);
				}
				return jobVertexId2exceptions;
			}
		).get();
		LOGGER.debug("get failovers of job {} from {} to {} : {}.", jobID, new Date(startTime).toString(), new Date(endTime).toString(), failovers);
		return failovers;
	}

	@Override
	public List<ExecutionVertexID> getTaskManagerTasks(String tmId) {
		final TaskmanagerAllSubtaskCurrentAttemptsInfoHeaders header = TaskmanagerAllSubtaskCurrentAttemptsInfoHeaders.getInstance();
		final TaskManagerMessageParameters parameters = header.getUnresolvedMessageParameters();
		final ResourceID  resourceId = new ResourceID(tmId);
		parameters.taskManagerIdParameter.resolve(resourceId);
		List<ExecutionVertexID> executionVertexIDs = new ArrayList<>();
		try {
			sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(TaskManagerExecutionVertexIdsInfo taskManagerExecutionVertexIdsInfo) -> {
					List<ExecutionVertexIDInfo> executionVertexIDInfos = taskManagerExecutionVertexIdsInfo.getExecutionVertexIds();
					if (executionVertexIDInfos != null && !executionVertexIDInfos.isEmpty()){
						executionVertexIDs.addAll(executionVertexIDInfos.stream().map(ExecutionVertexIDInfo::convertToResourceSpec).collect(Collectors.toList()));
					}
					return executionVertexIDs;
				}
			).get();
		} catch (Exception ignore) {
		}
		return executionVertexIDs;
	}

	@Override
	public Map<String, List<ExecutionVertexID>> getAllTaskManagerTasks() {
		final TaskmanagersAllSubtaskCurrentAttemptsInfoHeaders headers = TaskmanagersAllSubtaskCurrentAttemptsInfoHeaders.getInstance();
		final EmptyMessageParameters param = headers.getUnresolvedMessageParameters();
		Map<String, List<ExecutionVertexID>> result = new HashMap<>();
		try {
			sendRequest(headers, param, EmptyRequestBody.getInstance()).thenApply(
				(TaskManagersExecutionVertexIdsInfo taskManagersExecutionVertexIdsInfo) -> {
					Map<String, TaskManagerExecutionVertexIdsInfo> id2ExecutionVertexIds = taskManagersExecutionVertexIdsInfo.getExecutionVertexIds();
					for (Map.Entry<String, TaskManagerExecutionVertexIdsInfo> id2ExecutionVertexId: id2ExecutionVertexIds.entrySet()) {
						List<ExecutionVertexIDInfo> executionVertexIDInfos = id2ExecutionVertexId.getValue().getExecutionVertexIds();
						List<ExecutionVertexID> executionVertexIDs = new ArrayList<>();
						if (executionVertexIDInfos != null && !executionVertexIDInfos.isEmpty()){
							executionVertexIDs.addAll(executionVertexIDInfos.stream().map(ExecutionVertexIDInfo::convertToResourceSpec).collect(Collectors.toList()));
						}
						result.put(id2ExecutionVertexId.getKey(), executionVertexIDs);
					}
					return result;
				}
			).get();
		} catch (Exception e) {

		}
		return result;
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
		LOGGER.debug("Task metrics request of {}:\n{}", jobVertexID, metricNameList);
		try {
			sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
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
		LOGGER.debug("Task metrics:\n" + result);
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
		LOGGER.debug("Task Manager metrics request:\n" + metricNameList);
		Map<String, Map<String, Tuple2<Long, Double>>> result = new HashMap<>();
		try {
			sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(ComponentsMetricCollectionResponseBody cmc) -> {
					return updateMetricFromComponentsMetricCollection(cmc, result);
				}
			).get();
		} catch (Exception ignore) {
		}
		LOGGER.debug("Task Manager metrics:\n" + result);
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
		LOGGER.debug("Task Manager metrics request:\n" + metricNameList);
		Map<String, Map<String, Tuple2<Long, Double>>> result = new HashMap<>();
		try {
			sendRequest(header, parameters, EmptyRequestBody.getInstance()).thenApply(
				(ComponentsMetricCollectionResponseBody cmc) -> {
					return updateMetricFromComponentsMetricCollection(cmc, result);
				}
			).get();
		} catch (Exception ignore) {
		}
		LOGGER.debug("Task Manager metrics:\n" + result);
		return result;
	}

	@Override
	public void rescale(JobID jobId, Map<JobVertexID, Tuple2<Integer, ResourceSpec>> vertexParallelismResource) throws IOException {

		final UpdatingTriggerHeaders header = UpdatingTriggerHeaders.getInstance();
		final JobMessageParameters parameters = header.getUnresolvedMessageParameters();
		Map<String, UpdatingJobRequest.VertexResource> vertexParallelismResourceJsonMap = new HashMap<>();
		for (Map.Entry<JobVertexID, Tuple2<Integer, ResourceSpec>> id2resource: vertexParallelismResource.entrySet()){
			String idStr = id2resource.getKey().toString();
			Tuple2<Integer, ResourceSpec> parallism2Resource = id2resource.getValue();
			ResourceSpec resourceSpec = parallism2Resource.f1;
			Integer parallelism = parallism2Resource.f0;
			Map<String, ResourceSpecInfo.ResourceInfo> extendedResource = new HashMap<>();
			if (resourceSpec.getExtendedResources().size() > 0) {
				for (Map.Entry<String, Resource> name2Resource : resourceSpec.getExtendedResources().entrySet()) {
					extendedResource.put(name2Resource.getKey(), new ResourceSpecInfo.ResourceInfo(name2Resource.getValue()));
				}
			}
			ResourceSpecInfo resourceSpecInfo = new ResourceSpecInfo(
				resourceSpec.getCpuCores(),
				resourceSpec.getHeapMemory(),
				resourceSpec.getDirectMemory(),
				resourceSpec.getNativeMemory(),
				resourceSpec.getStateSize(),
				extendedResource
			);
			vertexParallelismResourceJsonMap.put(idStr, new UpdatingJobRequest.VertexResource(parallelism, resourceSpecInfo));
		}
		final UpdatingJobRequest updatingJobRequest = new UpdatingJobRequest(vertexParallelismResourceJsonMap);
		parameters.jobPathParameter.resolve(jobId);
		sendRequest(header, parameters, updatingJobRequest);
	}

	@Override
	public Map<Long, Exception> getTotalResourceLimitExceptions() throws Exception {
		final TotalResourceLimitExceptionInfosHeaders header = TotalResourceLimitExceptionInfosHeaders.getInstance();
		final EmptyMessageParameters param = header.getUnresolvedMessageParameters();
		Map<Long, Exception> result = new HashMap<>();
		return sendRequest(header, param, EmptyRequestBody.getInstance()).thenApply(
			(TotalResourceLimitExceptionsInfos totalResourceLimitInfos) -> {
				Map<Long, Exception> totalResourceLimit = totalResourceLimitInfos.getResourceLimit();
				if (totalResourceLimit != null && !totalResourceLimit.isEmpty()) {
					result.putAll(totalResourceLimit);
				}
				return totalResourceLimit;
			}
		).get();
	}

	@Override
	public Map<String, List<Exception>> getTaskManagerExceptions(long startTime, long endTime) throws Exception {
		final TaskManagerExceptionsHeaders header = TaskManagerExceptionsHeaders.getInstance();
		final EmptyMessageParameters param = header.getUnresolvedMessageParameters();
		Map<String, List<Exception>> result = new HashMap<>();
		return sendRequest(header, param, EmptyRequestBody.getInstance()).thenApply(
			(TaskManagerExceptionsInfos taskManagerExceptionsInfos) -> {
				Map<Long, TaskManagerExceptionsInfos.TaskManagerException> taskmanagerExceptions = taskManagerExceptionsInfos.getTaskmanagerExceptions();
				for (Map.Entry<Long, TaskManagerExceptionsInfos.TaskManagerException> time2Exception: taskmanagerExceptions.entrySet()) {
					if (time2Exception.getKey() < startTime || time2Exception.getKey() > endTime) {
						continue;
					}
					TaskManagerExceptionsInfos.TaskManagerException te = time2Exception.getValue();
					String tmId = te.getResourceId().getResourceIdString();
					if (!result.containsKey(tmId)) {
						result.put(tmId, new ArrayList<>());
					}
					result.get(tmId).add(te.getException());
				}
				return result;
			}
		).get();
	}

	@Override
	public Map<JobVertexID, TaskCheckpointStatistics> getJobVertexCheckPointStates(JobID jobId) throws Exception {
		final CheckpointingStatisticsHeaders header = CheckpointingStatisticsHeaders.getInstance();
		final JobMessageParameters param = header.getUnresolvedMessageParameters();
		param.jobPathParameter.resolve(jobId);
		CheckpointStatistics.CompletedCheckpointStatistics latestCheckpoints = sendRequest(header, param, EmptyRequestBody.getInstance()).get().getLatestCheckpoints().getCompletedCheckpointStatistics();
		Map<JobVertexID, TaskCheckpointStatistics> result = new HashMap<>();
		if (latestCheckpoints != null) {
			Long latestCheckpointId = latestCheckpoints.getId();
			final CheckpointStatisticDetailsHeaders checkpointStatisticDetailsHeaders = CheckpointStatisticDetailsHeaders.getInstance();
			final CheckpointMessageParameters checkpointMessageParameters = checkpointStatisticDetailsHeaders.getUnresolvedMessageParameters();
			checkpointMessageParameters.jobPathParameter.resolve(jobId);
			checkpointMessageParameters.checkpointIdPathParameter.resolve(latestCheckpointId);
			sendRequest(checkpointStatisticDetailsHeaders, checkpointMessageParameters, EmptyRequestBody.getInstance()).thenApply(
				(CheckpointStatistics checkpointStatistics) -> {
					result.putAll(checkpointStatistics.getCheckpointStatisticsPerTask());
					return result;
				}
			).get();
		}
		return result;
	}

	@Override
	public CheckpointStatistics getLatestCheckPointStates(JobID jobId) throws Exception {
		final CheckpointingStatisticsHeaders header = CheckpointingStatisticsHeaders.getInstance();
		final JobMessageParameters param = header.getUnresolvedMessageParameters();
		param.jobPathParameter.resolve(jobId);
		CheckpointStatistics.CompletedCheckpointStatistics latestCheckpoints = sendRequest(header, param, EmptyRequestBody.getInstance()).get().getLatestCheckpoints().getCompletedCheckpointStatistics();
		return latestCheckpoints;
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
