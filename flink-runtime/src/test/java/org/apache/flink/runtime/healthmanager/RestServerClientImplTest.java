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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
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
import org.apache.flink.runtime.rest.messages.job.JobAllSubtaskCurrentAttemptsInfoHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubtaskCurrentAttemptsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerExecutionVertexIdsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskmanagerAllSubtaskCurrentAttemptsInfoHeaders;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RestServerClientImpl}.
 *
 * <p>These tests verify that the client uses the appropriate headers for each
 * request, properly constructs the request bodies/parameters and processes the responses correctly.
 */
@PrepareForTest({RestServerClientImpl.class})
public class RestServerClientImplTest extends TestLogger {

	private static final String REST_ADDRESS = "http://localhost:1234";

	@Mock
	private Dispatcher mockRestfulGateway;

	@Mock
	private GatewayRetriever<DispatcherGateway> mockGatewayRetriever;

	private RestServerEndpointConfiguration restServerEndpointConfiguration;

	private RestServerClientImpl restServerClientImpl;

	private ExecutorService executor;

	private JobGraph jobGraph;

	private JobID jobId;

	private ResourceSpec resourceSpec = new ResourceSpec.Builder().setCpuCores(1)
		.setHeapMemoryInMB(10)
		.setNativeMemoryInMB(20)
		.setNativeMemoryInMB(30)
		.setDirectMemoryInMB(40)
		.setStateSizeInMB(50).build();

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(REST_ADDRESS));

		final Configuration config = new Configuration();
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(JobManagerOptions.PORT, 1234);
		config.setString(RestOptions.ADDRESS, "localhost");
		config.setInteger(RestOptions.PORT, 1234);
		config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
		config.setLong(RestOptions.RETRY_DELAY, 0);

		restServerEndpointConfiguration = RestServerEndpointConfiguration.fromConfiguration(config);
		mockGatewayRetriever = () -> CompletableFuture.completedFuture(mockRestfulGateway);

		executor = new ScheduledThreadPoolExecutor(
			4, new ExecutorThreadFactory("health-manager"));
		restServerClientImpl = new RestServerClientImpl(REST_ADDRESS, config, executor);
		jobGraph = createBlockingJob(10, resourceSpec);
		jobId = jobGraph.getJobID();
	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testListJobs() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestListJobsHandler())) {
			{
				List<JobStatusMessage> jobDetails = restServerClientImpl.listJob();
				Iterator<JobStatusMessage> jobDetailsIterator = jobDetails.iterator();
				JobStatusMessage job1 = jobDetailsIterator.next();
				JobStatusMessage job2 = jobDetailsIterator.next();
				Assert.assertNotEquals("The job status should not be equal.", job1.getJobState(), job2.getJobState());
			}
		}
	}

	@Test
	public void testGetJobConfig() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestJobGraphOverviewHandler())) {
			{
				RestServerClient.JobConfig jobConfig = restServerClientImpl.getJobConfig(jobId);
				for (Map.Entry<JobVertexID, RestServerClient.VertexConfig> id2vertex: jobConfig.getVertexConfigs().entrySet()) {
					Assert.assertEquals(id2vertex.getValue().getResourceSpec(), resourceSpec);
				}
				Assert.assertTrue(jobConfig.getConfig().toMap().size() == 5);
				Assert.assertNotNull(jobConfig);
			}
		}
	}

	@Test
	public void testGetTaskManagerTasks() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestTaskmanagerAllSubtaskCurrentAttemptsHandler())) {
			{
				final ResourceID resourceId = new ResourceID("12233");
				List<ExecutionVertexID> executionVertexIDS = restServerClientImpl.getTaskManagerTasks(resourceId.toString());
				Assert.assertTrue(executionVertexIDS.size() == 3);
			}
		}
	}

	@Test
	public void testGetJobStatus() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestJobAllSubtaskCurrentAttemptsHandler())) {
			{
				RestServerClient.JobStatus jobStatus = restServerClientImpl.getJobStatus(jobId);
				Assert.assertTrue(jobStatus.getTaskStatus().size() > 0);
			}
		}
	}

	@Test
	public void testGetFailover() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestJobExceptionsHandler())) {
			{
				Map<JobVertexID, List<JobException>> failovers = restServerClientImpl.getFailover(jobId, System.currentTimeMillis(), System.currentTimeMillis());
				Assert.assertTrue(failovers.size() > 0);
			}
		}
	}

	@Test
	public void testGetTotalResourceLimitExceptions() throws Exception {
		try (TestRestServerEndpoint ignored = createRestServerEndpoint(new TestTotalResourceLimitExceptionsHandler())) {
			{
				Map<Long, Exception> exceptions = restServerClientImpl.getTotalResourceLimitExceptions();
				Assert.assertTrue(exceptions.size() == 1);
			}
		}
	}

	private class TestListJobsHandler extends TestHandler<EmptyRequestBody, MultipleJobsDetails, EmptyMessageParameters> {

		private TestListJobsHandler() {
			super(JobsOverviewHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<MultipleJobsDetails> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			JobDetails running = new JobDetails(new JobID(), "job1", 0, 0, 0, JobStatus.RUNNING, 0, new int[9], 0);
			JobDetails finished = new JobDetails(new JobID(), "job2", 0, 0, 0, JobStatus.FINISHED, 0, new int[9], 0);
			return CompletableFuture.completedFuture(new MultipleJobsDetails(Arrays.asList(running, finished)));
		}
	}

	private class TestJobGraphOverviewHandler extends TestHandler<EmptyRequestBody, JobGraphOverviewInfo, JobMessageParameters> {
		private TestJobGraphOverviewHandler() {
			super(JobGraphOverviewHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobGraphOverviewInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			final JobGraphOverviewInfo jobGraphOverviewInfo = createInfo(jobGraph);
			return CompletableFuture.completedFuture(jobGraphOverviewInfo);
		}

	}

	private class TestTaskmanagerAllSubtaskCurrentAttemptsHandler extends TestHandler<EmptyRequestBody, TaskManagerExecutionVertexIdsInfo, TaskManagerMessageParameters> {
		private TestTaskmanagerAllSubtaskCurrentAttemptsHandler() {
			super(TaskmanagerAllSubtaskCurrentAttemptsInfoHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<TaskManagerExecutionVertexIdsInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			List<ExecutionVertexIDInfo> executionVertexIds = new ArrayList<>();
			ExecutionVertexIDInfo executionVertexIDInfo1 = new ExecutionVertexIDInfo(new JobVertexID(), 0);
			ExecutionVertexIDInfo executionVertexIDInfo2 = new ExecutionVertexIDInfo(new JobVertexID(), 0);
			ExecutionVertexIDInfo executionVertexIDInfo3 = new ExecutionVertexIDInfo(new JobVertexID(), 0);
			executionVertexIds.add(executionVertexIDInfo1);
			executionVertexIds.add(executionVertexIDInfo2);
			executionVertexIds.add(executionVertexIDInfo3);
			TaskManagerExecutionVertexIdsInfo taskManagerExecutionVertexIdsInfo = new TaskManagerExecutionVertexIdsInfo(executionVertexIds);
			return CompletableFuture.completedFuture(taskManagerExecutionVertexIdsInfo);
		}

	}

	private class TestJobAllSubtaskCurrentAttemptsHandler extends TestHandler<EmptyRequestBody, JobSubtaskCurrentAttemptsInfo, JobMessageParameters> {
		private TestJobAllSubtaskCurrentAttemptsHandler() {
			super(JobAllSubtaskCurrentAttemptsInfoHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobSubtaskCurrentAttemptsInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			Collection<SubtaskExecutionAttemptInfo> subtaskExecutionAttemptsInfo = new ArrayList<>();
			try {
				final ExecutionGraph executionGraph = ExecutionGraphTestUtils.createExecutionGraph(
					jobGraph,
					new SimpleAckingTaskManagerGateway(),
					new NoRestartStrategy());
				for (AccessExecutionJobVertex accessExecutionJobVertex : executionGraph.getVerticesTopologically()) {
					String vertexId = accessExecutionJobVertex.getJobVertexId().toString();
					for (AccessExecutionVertex executionVertex: accessExecutionJobVertex.getTaskVertices()) {
						final AccessExecution execution = executionVertex.getCurrentExecutionAttempt();
						SubtaskExecutionAttemptInfo subtaskExecutionAttemptInfo = SubtaskExecutionAttemptInfo.create(execution, vertexId);
						subtaskExecutionAttemptsInfo.add(subtaskExecutionAttemptInfo);
					}
				}
				return CompletableFuture.completedFuture(new JobSubtaskCurrentAttemptsInfo(executionGraph.getState(), subtaskExecutionAttemptsInfo));
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

	}

	private class TestJobExceptionsHandler extends TestHandler<EmptyRequestBody, JobExceptionsInfo, JobExceptionsMessageParameters> {
		private TestJobExceptionsHandler() {
			super(JobExceptionsHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<JobExceptionsInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobExceptionsMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			List<JobExceptionsInfo.ExecutionExceptionInfo> executionTaskExceptionInfoList = new ArrayList<>();
			executionTaskExceptionInfoList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
				"exception1",
				"task1",
				"location1",
				System.currentTimeMillis(),
				(new JobVertexID()).toString(),
				1,
				0));
			executionTaskExceptionInfoList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
				"exception2",
				"task2",
				"location2",
				System.currentTimeMillis(),
				(new JobVertexID()).toString(),
				1,
				1));
			return CompletableFuture.completedFuture(new JobExceptionsInfo(
				"root exception",
				System.currentTimeMillis(),
				executionTaskExceptionInfoList,
				false));
		}

	}

	private class TestTotalResourceLimitExceptionsHandler extends TestHandler<EmptyRequestBody, TotalResourceLimitExceptionsInfos, EmptyMessageParameters> {
		private TestTotalResourceLimitExceptionsHandler() {
			super(TotalResourceLimitExceptionInfosHeaders.getInstance());
		}

		@Override
		protected CompletableFuture<TotalResourceLimitExceptionsInfos> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
			Map<Long, Exception> exceptionMap = new HashMap<>();
			exceptionMap.put(System.currentTimeMillis(), new Exception("aa"));
			TotalResourceLimitExceptionsInfos exceptionsInfos = new TotalResourceLimitExceptionsInfos(exceptionMap);
			return CompletableFuture.completedFuture(exceptionsInfos);
		}

	}

	private abstract class TestHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends AbstractRestHandler<DispatcherGateway, R, P, M> {

		private TestHandler(MessageHeaders<R, P, M> headers) {
			super(
				CompletableFuture.completedFuture(REST_ADDRESS),
				mockGatewayRetriever,
				RpcUtils.INF_TIMEOUT,
				Collections.emptyMap(),
				headers);
		}
	}

	private TestRestServerEndpoint createRestServerEndpoint(
			final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws Exception {
		final TestRestServerEndpoint testRestServerEndpoint = new TestRestServerEndpoint(abstractRestHandlers);
		testRestServerEndpoint.start();
		return testRestServerEndpoint;
	}

	private class TestRestServerEndpoint extends RestServerEndpoint implements AutoCloseable {

		private final AbstractRestHandler<?, ?, ?, ?>[] abstractRestHandlers;

		TestRestServerEndpoint(final AbstractRestHandler<?, ?, ?, ?>... abstractRestHandlers) throws IOException {
			super(restServerEndpointConfiguration);
			this.abstractRestHandlers = abstractRestHandlers;
		}

		@Override
		protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>>
				initializeHandlers(CompletableFuture<String> restAddressFuture) {
			final List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>();
			for (final AbstractRestHandler abstractRestHandler : abstractRestHandlers) {
				handlers.add(Tuple2.of(
					abstractRestHandler.getMessageHeaders(),
					abstractRestHandler));
			}
			return handlers;
		}

		@Override
		protected void startInternal() throws Exception {}
	}

	public JobGraph createBlockingJob(int parallelism, ResourceSpec resourceSpec) {
		Tasks.BlockingOnceReceiver$.MODULE$.blocking_$eq(true);

		JobVertex sender = new JobVertex("sender");
		JobVertex receiver = new JobVertex("receiver");

		sender.setInvokableClass(Tasks.Sender.class);
		receiver.setInvokableClass(Tasks.BlockingOnceReceiver.class);

		sender.setParallelism(parallelism);
		receiver.setParallelism(parallelism);

		sender.setResources(resourceSpec, resourceSpec);
		receiver.setResources(resourceSpec, resourceSpec);

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);
		Configuration configuration = new Configuration();
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 1 * 32 * 1024);
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 1 * 32 * 1024);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 2);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, 2);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 0);

		JobGraph jobGraphTmp = new JobGraph("Blocking test job", sender, receiver);
		jobGraphTmp.addCustomConfiguration(configuration);
		return jobGraphTmp;
	}

	private JobGraphOverviewInfo createInfo(JobGraph jobGraph) {
		Configuration config = jobGraph.getJobConfiguration();
		Map<String, JobGraphOverviewInfo.VertexConfigInfo> vertexConfigs = new HashMap<>();
		Map<String, List<JobGraphOverviewInfo.EdgeConfigInfo>> inputNodes = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			List<Integer> nodeIds;
			JobVertexID vertexID = vertex.getID();
			if (vertex.getOperatorDescriptors() != null) {
				nodeIds = vertex.getOperatorDescriptors().stream().map(op -> op.getNodeId()).collect(Collectors.toList());
			} else {
				nodeIds = new ArrayList<>();
			}
			List<JobGraphOverviewInfo.EdgeConfigInfo> inputVertexId;
			if (vertex.getInputs() != null) {
				inputVertexId = vertex.getInputs().stream().map(edge ->
					new JobGraphOverviewInfo.EdgeConfigInfo(edge.getSource().getProducer().getID().toString(), edge.getShipStrategyName())
				).collect(Collectors.toList());
			} else {
				inputVertexId = new ArrayList<>();
			}
			ResourceSpec resourceSpec = vertex.getMinResources();
			ResourceSpecInfo resourceSpecInfo = new ResourceSpecInfo(
				resourceSpec.getCpuCores(),
				resourceSpec.getHeapMemory(),
				resourceSpec.getDirectMemory(),
				resourceSpec.getNativeMemory(),
				resourceSpec.getStateSize(),
				resourceSpec.getExtendedResources()
			);
			AbstractID coLocationGroupId = vertex.getCoLocationGroup() != null ? vertex.getCoLocationGroup().getId() : null;
			JobGraphOverviewInfo.VertexConfigInfo vertexConfigInfo = new JobGraphOverviewInfo.VertexConfigInfo(vertexID, vertex.getName(),
				vertex.getParallelism(), vertex.getMaxParallelism(), resourceSpecInfo, nodeIds, coLocationGroupId
			);
			vertexConfigs.put(vertexID.toString(), vertexConfigInfo);
			inputNodes.put(vertexID.toString(), inputVertexId);
		}
		return new JobGraphOverviewInfo(config.toMap(), vertexConfigs, inputNodes);
	}

}
