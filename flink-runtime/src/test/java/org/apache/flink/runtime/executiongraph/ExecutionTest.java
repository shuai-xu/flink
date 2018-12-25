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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link Execution}.
 */
public class ExecutionTest extends TestLogger {

	/**
	 * Tests that slots are released if we cannot assign the allocated resource to the
	 * Execution.
	 */
	@Test
	public void testSlotReleaseOnFailedResourceAssignment() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, slotFuture);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final LogicalSlot otherSlot = new TestingLogicalSlot();

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertFalse(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		// assign a different resource to the execution
		assertTrue(execution.tryAssignResource(otherSlot));

		// completing now the future should cause the slot to be released
		slotFuture.complete(slot);

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that the slot is released in case of a execution cancellation when having
	 * a slot assigned and being in state SCHEDULED.
	 */
	@Test
	public void testSlotReleaseOnExecutionCancellationInScheduled() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		// cancelling the execution should move it into state CANCELED
		execution.cancel();
		assertEquals(ExecutionState.CANCELED, execution.getState());

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that the slot is released in case of a execution cancellation when being in state
	 * RUNNING.
	 */
	@Test
	public void testSlotReleaseOnExecutionCancellationInRunning() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		execution.deploy();

		execution.switchToRunning();

		// cancelling the execution should move it into state CANCELING
		execution.cancel();
		assertEquals(ExecutionState.CANCELING, execution.getState());

		execution.cancelingComplete();

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that a slot allocation from a {@link SlotProvider} is cancelled if the
	 * {@link Execution} is cancelled.
	 */
	@Test
	public void testSlotAllocationCancellationWhenExecutionCancelled() throws Exception {
		final JobVertexID jobVertexId = new JobVertexID();
		final JobVertex jobVertex = new JobVertex("test vertex", jobVertexId);
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		final CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();
		slotProvider.addSlot(jobVertexId, 0, slotFuture);

		final ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		final ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		final CompletableFuture<Execution> allocationFuture = currentExecutionAttempt.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertThat(allocationFuture.isDone(), is(false));

		assertThat(slotProvider.getSlotRequestedFuture(jobVertexId, 0).get(), is(true));

		final Set<SlotRequestId> slotRequests = slotProvider.getSlotRequests();
		assertThat(slotRequests, hasSize(1));

		assertThat(currentExecutionAttempt.getState(), is(ExecutionState.SCHEDULED));

		currentExecutionAttempt.cancel();
		assertThat(currentExecutionAttempt.getState(), is(ExecutionState.CANCELED));

		assertThat(allocationFuture.isCompletedExceptionally(), is(true));

		final Set<SlotRequestId> canceledSlotRequests = slotProvider.getCanceledSlotRequests();
		assertThat(canceledSlotRequests, equalTo(slotRequests));
	}

	/**
	 * Tests that all preferred locations are calculated.
	 */
	@Test
	public void testAllPreferredLocationCalculation() throws ExecutionException, InterruptedException {
		final TaskManagerLocation taskManagerLocation1 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation2 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation3 = new LocalTaskManagerLocation();

		final CompletableFuture<TaskManagerLocation> locationFuture1 = CompletableFuture.completedFuture(taskManagerLocation1);
		final CompletableFuture<TaskManagerLocation> locationFuture2 = new CompletableFuture<>();
		final CompletableFuture<TaskManagerLocation> locationFuture3 = new CompletableFuture<>();

		final Execution execution = SchedulerTestUtils.getTestVertex(Arrays.asList(locationFuture1, locationFuture2, locationFuture3));

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = execution.calculatePreferredLocations(LocationPreferenceConstraint.ALL);

		assertFalse(preferredLocationsFuture.isDone());

		locationFuture3.complete(taskManagerLocation3);

		assertFalse(preferredLocationsFuture.isDone());

		locationFuture2.complete(taskManagerLocation2);

		assertTrue(preferredLocationsFuture.isDone());

		final Collection<TaskManagerLocation> preferredLocations = preferredLocationsFuture.get();

		assertThat(preferredLocations, containsInAnyOrder(taskManagerLocation1, taskManagerLocation2, taskManagerLocation3));
	}

	/**
	 * Tests that any preferred locations are calculated.
	 */
	@Test
	public void testAnyPreferredLocationCalculation() throws ExecutionException, InterruptedException {
		final TaskManagerLocation taskManagerLocation1 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation3 = new LocalTaskManagerLocation();

		final CompletableFuture<TaskManagerLocation> locationFuture1 = CompletableFuture.completedFuture(taskManagerLocation1);
		final CompletableFuture<TaskManagerLocation> locationFuture2 = new CompletableFuture<>();
		final CompletableFuture<TaskManagerLocation> locationFuture3 = CompletableFuture.completedFuture(taskManagerLocation3);

		final Execution execution = SchedulerTestUtils.getTestVertex(Arrays.asList(locationFuture1, locationFuture2, locationFuture3));

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = execution.calculatePreferredLocations(LocationPreferenceConstraint.ANY);

		assertTrue(preferredLocationsFuture.isDone());

		final Collection<TaskManagerLocation> preferredLocations = preferredLocationsFuture.get();

		assertThat(preferredLocations, containsInAnyOrder(taskManagerLocation1, taskManagerLocation3));
	}

	/**
	 * Checks that the {@link Execution} termination future is only completed after the
	 * assigned slot has been released.
	 *
	 * <p>NOTE: This test only fails spuriously without the fix of this commit. Thus, one has
	 * to execute this test multiple times to see the failure.
	 */
	@Test
	public void testTerminationFutureIsCompletedAfterSlotRelease() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId),
			slotOwner);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		executionVertex.scheduleForExecution(slotProvider, false, LocationPreferenceConstraint.ANY).get();

		Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

		CompletableFuture<LogicalSlot> returnedSlotFuture = slotOwner.getReturnedSlotFuture();
		CompletableFuture<?> terminationFuture = executionVertex.cancel();

		// run canceling in a separate thread to allow an interleaving between termination
		// future callback registrations
		CompletableFuture.runAsync(
			() -> currentExecutionAttempt.cancelingComplete(),
			TestingUtils.defaultExecutor());

		// to increase probability for problematic interleaving, let the current thread yield the processor
		Thread.yield();

		CompletableFuture<Boolean> restartFuture = terminationFuture.thenApply(
			ignored -> {
				assertTrue(returnedSlotFuture.isDone());
				return true;
			});


		// check if the returned slot future was completed first
		restartFuture.get();
	}

	/**
	 * Tests that the task restore state is nulled after the {@link Execution} has been
	 * deployed. See FLINK-9693.
	 */
	@Test
	public void testTaskRestoreStateIsNulledAfterDeployment() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId),
			slotOwner);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			new DirectScheduledExecutorService(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		final Execution execution = executionVertex.getCurrentExecutionAttempt();

		final JobManagerTaskRestore taskRestoreState = new JobManagerTaskRestore(1L, new TaskStateSnapshot());
		execution.setInitialState(taskRestoreState);

		assertThat(execution.getTaskRestore(), is(notNullValue()));

		// schedule the execution vertex and wait for its deployment
		executionVertex.scheduleForExecution(slotProvider, false, LocationPreferenceConstraint.ANY).get();

		assertThat(execution.getTaskRestore(), is(nullValue()));
	}

	@Test
	public void testResourceCalculationBeforeSlotAllocationInScheduled() throws Exception {
		// Prepares input gates and input channels.
		// 0. Prepares for various settings.
		final int numPipelineChannelsPerGate = 10;
		final int numPipelineGates = 2;
		final int numConsumersPerExternalResultPartition = 10;
		final int numExternalResultPartitions = 3;

		final int networkBuffersPerChannel = 100;
		final int networkBuffersPerSubpartition = 50;
		final int networkExtraBuffersPerGate = 20;
		final int taskManagerOutputMemoryMB = 170;

		final float cpuCores = 1.0f;
		final int heapMemoryInMB = 1024;
		final int directMemoryInMB = 200;
		final int nativeMemoryInMB = 100;
		final int managedMemoryInMB = 1000;

		// 1. Prepares for pipelined input edges.
		IntermediateResult mockPipelinedIntermediateResult = new IntermediateResult(
			new IntermediateDataSetID(),
			mock(ExecutionJobVertex.class),
			numPipelineChannelsPerGate,
			ResultPartitionType.PIPELINED);

		IntermediateResultPartition mockPipelinedIntermediateResultPartition =
			mock(IntermediateResultPartition.class);
		when(mockPipelinedIntermediateResultPartition.getIntermediateResult())
			.thenReturn(mockPipelinedIntermediateResult);

		ExecutionEdge mockExecutionEdge = mock(ExecutionEdge.class);
		when(mockExecutionEdge.getSource()).thenReturn(mockPipelinedIntermediateResultPartition);

		ExecutionEdge[][] inputEdges = new ExecutionEdge[numPipelineGates][numPipelineChannelsPerGate];
		for (int i = 0; i < numPipelineGates; i++) {
			ExecutionEdge[] edgesPerGate = new ExecutionEdge[numPipelineChannelsPerGate];
			for (int j = 0; j < numPipelineChannelsPerGate; j++) {
				edgesPerGate[j] = mockExecutionEdge;
			}
			inputEdges[i] = edgesPerGate;
		}

		// 2. Prepares for blocking output edges using external shuffle service.
		IntermediateResult mockBlockingIntermediateResult = new IntermediateResult(
			new IntermediateDataSetID(),
			mock(ExecutionJobVertex.class),
			numPipelineChannelsPerGate,
			ResultPartitionType.BLOCKING);

		IntermediateResultPartition mockBlockingIntermediateResultPartition =
			mock(IntermediateResultPartition.class);
		when(mockBlockingIntermediateResultPartition.getIntermediateResult())
			.thenReturn(mockBlockingIntermediateResult);
		List<List<ExecutionEdge>> consumersPerExternalResultPartition = new ArrayList<>();
		consumersPerExternalResultPartition.add(new ArrayList<>());
		for (int i = 0; i < numConsumersPerExternalResultPartition; i++) {
			consumersPerExternalResultPartition.get(0).add(mockExecutionEdge);
		}
		when(mockBlockingIntermediateResultPartition.getConsumers())
			.thenReturn(consumersPerExternalResultPartition);

		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedPartitions =
			new HashMap<>();
		for (int i = 0; i < numExternalResultPartitions; i++) {
			producedPartitions.put(new IntermediateResultPartitionID(),
				mockBlockingIntermediateResultPartition);
		}

		// 3. Prepares other facilities for this unittest.
		final JobVertex jobVertex = spy(createNoOpJobVertex());
		when(jobVertex.getMinResources()).thenReturn(ResourceSpec.newBuilder()
			.setCpuCores(cpuCores)
			.setHeapMemoryInMB(heapMemoryInMB)
			.setDirectMemoryInMB(directMemoryInMB)
			.setNativeMemoryInMB(nativeMemoryInMB)
			.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB))
			.build());

		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final AtomicReference<SlotProfile> actualSlotProfile = new AtomicReference(null);

		final ProgrammedSlotProvider slotProvider = spy(new ProgrammedSlotProvider(1) {
			@Override
			public CompletableFuture<LogicalSlot> allocateSlot(
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				boolean allowQueued,
				SlotProfile slotProfile,
				Time allocationTimeout) {

				actualSlotProfile.set(slotProfile);
				return super.allocateSlot(
					slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
			}
		});

		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, networkBuffersPerChannel);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, networkBuffersPerSubpartition);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, networkExtraBuffersPerGate);
		jobManagerConfiguration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE,
			BlockingShuffleType.YARN.toString());
		jobManagerConfiguration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, taskManagerOutputMemoryMB);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			jobManagerConfiguration,
			slotProvider,
			new NoRestartStrategy(),
			TestingUtils.defaultExecutor(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = spy(executionGraph.getJobVertex(jobVertexId));

		final Execution execution = spy(executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt());

		ExecutionVertex executionVertex = spy(execution.getVertex());
		when(executionVertex.getNumberOfInputs()).thenReturn(inputEdges.length);
		doAnswer(new Answer<ExecutionEdge[]>() {
			@Override
			public ExecutionEdge[] answer(InvocationOnMock invocation) {
				int index = invocation.getArgumentAt(0, int.class);
				return inputEdges[index];
			}
		}).when(executionVertex).getInputEdges(any(int.class));
		when(executionVertex.getProducedPartitions()).thenReturn(producedPartitions);

		when(execution.getVertex()).thenReturn(executionVertex);

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		// cancelling the execution should move it into state CANCELED
		execution.cancel();
		assertEquals(ExecutionState.CANCELED, execution.getState());

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());

		assertTrue(actualSlotProfile.get() != null);
		assertEquals((int) Math.ceil(1.0 * (networkBuffersPerChannel * numPipelineChannelsPerGate * numPipelineGates
			+ networkExtraBuffersPerGate * numPipelineGates) * 32 / 1024),
			actualSlotProfile.get().getResourceProfile().getNetworkMemoryInMB());
		assertEquals(managedMemoryInMB + taskManagerOutputMemoryMB * numExternalResultPartitions,
			actualSlotProfile.get().getResourceProfile().getManagedMemoryInMB());
	}

	@Test
	public void testNetworkMemoryCalculation() throws Exception {
		int[][] parameters = {{2, 8, 128, 2, 10, 10 * 128 + 2 * 2 + 2 * 10 + 8},
			{2, 8, 128, 2, 1, 2 * 128 + 2 * 2 + 2 * 10 + 8}};

		for (int[] parameter: parameters) {
			testNetworkMemoryCalculation(parameter);
		}
	}

	private void testNetworkMemoryCalculation(int[] parameters) throws Exception {
		final int NETWORK_BUFFERS_PER_CHANNEL = parameters[0];
		final int NETWORK_EXTRA_BUFFERS_PER_GATE = parameters[1];
		final int NETWORK_BUFFERS_PER_BLOCKING_CHANNEL = parameters[2];
		final int NETWORK_EXTRA_BUFFERS_PER_BLOCKING_GATE = parameters[3];
		final int YARN_SHUFFLE_SERVICE_MAX_REQUESTS_IN_FLIGHT = parameters[4];

		final JobVertex jobVertex1 = createNoOpJobVertex();
		final JobVertexID jobVertexId1 = jobVertex1.getID();

		final JobVertex jobVertex2 = createNoOpJobVertex();
		jobVertex2.setParallelism(10);

		final JobVertex jobVertex3 = createNoOpJobVertex();
		jobVertex3.setParallelism(10);

		final JobVertex jobVertex4 = createNoOpJobVertex();
		jobVertex4.setParallelism(10);

		final Configuration configuration = new Configuration();
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, NETWORK_BUFFERS_PER_CHANNEL);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, NETWORK_EXTRA_BUFFERS_PER_GATE);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL, NETWORK_BUFFERS_PER_BLOCKING_CHANNEL);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_EXTERNAL_BLOCKING_GATE, NETWORK_EXTRA_BUFFERS_PER_BLOCKING_GATE);
		configuration.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS, YARN_SHUFFLE_SERVICE_MAX_REQUESTS_IN_FLIGHT);
		configuration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, BlockingShuffleType.YARN.toString());
		configuration.setInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE, 1024 * 1024);

		IntermediateDataSetID dataSetID1 = new IntermediateDataSetID();
		IntermediateDataSet dataSet1 = mock(IntermediateDataSet.class);
		when(dataSet1.getId()).thenReturn(dataSetID1);

		IntermediateDataSetID dataSetID2 = new IntermediateDataSetID();
		IntermediateDataSet dataSet2 = mock(IntermediateDataSet.class);
		when(dataSet2.getId()).thenReturn(dataSetID2);

		IntermediateDataSetID dataSetID3 = new IntermediateDataSetID();
		IntermediateDataSet dataSet3 = mock(IntermediateDataSet.class);
		when(dataSet3.getId()).thenReturn(dataSetID3);

		DistributionPattern distributionPattern = DistributionPattern.ALL_TO_ALL;

		jobVertex4.connectDataSetAsInput(jobVertex1, dataSet1.getId(), distributionPattern, ResultPartitionType.BLOCKING);

		jobVertex4.connectDataSetAsInput(jobVertex2, dataSet2.getId(), distributionPattern, ResultPartitionType.PIPELINED);

		jobVertex4.connectDataSetAsInput(jobVertex3, dataSet3.getId(), distributionPattern, ResultPartitionType.BLOCKING);

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId1),
			slotOwner);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			configuration,
			slotProvider,
			new NoRestartStrategy(),
			TestingUtils.defaultExecutor(),
			new JobVertex[] {jobVertex1, jobVertex2, jobVertex3, jobVertex4});

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertex4.getID());

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		final Execution execution = executionVertex.getCurrentExecutionAttempt();

		assertEquals(parameters[5], execution.calculateTaskNetworkMemory(executionVertex));
	}

	@Nonnull
	private JobVertex createNoOpJobVertex() {
		final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
		jobVertex.setInvokableClass(NoOpInvokable.class);

		return jobVertex;
	}

	@Nonnull
	private ProgrammedSlotProvider createProgrammedSlotProvider(
		int parallelism,
		Collection<JobVertexID> jobVertexIds,
		SlotOwner slotOwner) {
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);

		for (JobVertexID jobVertexId : jobVertexIds) {
			for (int i = 0; i < parallelism; i++) {
				final SimpleSlot slot = new SimpleSlot(
					slotOwner,
					new LocalTaskManagerLocation(),
					0,
					new SimpleAckingTaskManagerGateway(),
					null,
					null);

				slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));
			}
		}

		return slotProvider;
	}

	/**
	 * Slot owner which records the first returned slot.
	 */
	private static final class SingleSlotTestingSlotOwner implements SlotOwner {

		final CompletableFuture<LogicalSlot> returnedSlot = new CompletableFuture<>();

		public CompletableFuture<LogicalSlot> getReturnedSlotFuture() {
			return returnedSlot;
		}

		@Override
		public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {
			return CompletableFuture.completedFuture(returnedSlot.complete(logicalSlot));
		}
	}
}
