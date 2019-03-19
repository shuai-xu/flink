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

package org.apache.flink.runtime.schedule;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.SchedulingMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.BestEffortExecutionSlotAllocator;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionVertexState;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link StepwiseSchedulingPlugin}.
 */
public class ConcurrentGroupGraphManagerPluginTest extends GraphManagerPluginTestBase {

	/**
	 * Tests build scheduling groups for a simple all to all streaming job.
	 */
	@Test
	public void testBuildGroupsForSimpleAllToAllJob() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setParallelism(3);
		v2.setParallelism(4);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		v3.setParallelism(2);
		v4.setParallelism(1);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3, v4);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());
		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
			eg,
			null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(1, schedulingGroups.size());
		assertEquals(10, schedulingGroups.iterator().next().getExecutionVertices().size());
	}

	/**
	 * Tests build scheduling groups for a simple point wise streaming job.
	 */
	@Test
	public void testBuildGroupsForSimplePointWiseJob() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setParallelism(3);
		v2.setParallelism(3);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		v3.setParallelism(2);
		v4.setParallelism(1);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3, v4);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());
		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				scheduler,
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(1, schedulingGroups.size());
		assertEquals(9, schedulingGroups.iterator().next().getExecutionVertices().size());
	}

	/**
	 * Tests build scheduling groups for a multi point wise streaming job.
	 */
	@Test
	public void testBuildGroupsForMultiPointWiseJob() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		v1.setParallelism(4);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());
		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				scheduler,
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(1, schedulingGroups.size());
		assertEquals(8, schedulingGroups.iterator().next().getExecutionVertices().size());
	}

	/**
	 * Tests build scheduling groups for a simple point wise batch job.
	 */
	@Test
	public void testBuildGroupsForSimplePointWiseBatchJob() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setParallelism(3);
		v2.setParallelism(3);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		v2.getInputs().get(0).setSchedulingMode(SchedulingMode.SEQUENTIAL);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				scheduler,
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(6, schedulingGroups.size());
		assertEquals(1, schedulingGroups.iterator().next().getExecutionVertices().size());
	}

	/**
	 * Tests build scheduling groups for a batch job with control edges.
	 */
	@Test
	public void testBuildGroupsForBatchJobWithControlEdge() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		final JobVertex v5 = new JobVertex("vertex5");
		final JobVertex v6 = new JobVertex("vertex6");
		final JobVertex v7 = new JobVertex("vertex7");
		final JobVertex v8 = new JobVertex("vertex8");
		final JobVertex v9 = new JobVertex("vertex9");
		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);
		v5.setParallelism(2);
		v6.setParallelism(2);
		v7.setParallelism(2);
		v8.setParallelism(2);
		v9.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);
		v6.setInvokableClass(AbstractInvokable.class);
		v7.setInvokableClass(AbstractInvokable.class);
		v8.setInvokableClass(AbstractInvokable.class);
		v9.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v6.connectNewDataSetAsInput(v5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v7.connectNewDataSetAsInput(v6, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v8.connectNewDataSetAsInput(v7, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v8, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v9.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		v8.connectControlEdge(v3, ControlType.START_ON_FINISH);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3, v4, v5, v6, v7, v8, v9);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				scheduler,
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(6, schedulingGroups.size());
	}

	/**
	 * Tests build scheduling groups for a batch job with multi input edges between two vertices.
	 */
	@Test
	public void testBuildGroupsForBatchJobWithMultiInputEdge() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v4.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectDataSetAsInput(v2, new IntermediateDataSetID(), DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		v3.connectControlEdge(v1, ControlType.START_ON_FINISH);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3, v4);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				new SimpleAckingTaskManagerGateway(),
				new NoRestartStrategy());

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(null, Collections.EMPTY_LIST);

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				scheduler,
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				null);

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(2, schedulingGroups.size());
	}

	/**
	 * Tests scheduling job in concurrent groups.
	 */
	@Test
	public void testScheduleByConcurrentGroups() throws Exception {

		int parallelism = 2;

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		final JobVertex v5 = new JobVertex("vertex5");
		v1.setParallelism(parallelism);
		v2.setParallelism(parallelism);
		v3.setParallelism(parallelism);
		v4.setParallelism(parallelism);
		v5.setParallelism(parallelism);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v5.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		v2.connectControlEdge(v3, ControlType.START_ON_FINISH);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", v1, v2, v3, v4, v5);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		List<ExecutionVertex> executionVertices = new ArrayList<>();
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			executionVertices.add(ev);
		}
		final TestExecutionVertexScheduler scheduler = spy(new TestExecutionVertexScheduler(eg, executionVertices));

		final TestingExecutionSlotAllocator allocator = new TestingExecutionSlotAllocator(eg.getSlotProvider());
		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
			eg,
			eg.getGraphManager(),
			allocator);

		graphManagerPlugin.onSchedulingStarted();
		assertEquals(2, allocator.getScheduledVertices().size());
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v1.getID(), 0)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v1.getID(), 1)));

		allocator.clearScheduledVertices();

		// Set partition consumable
		for (int i = 0; i < v1.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(v1.getID())
					.getTaskVertices()[0].getProducedPartitions().values()) {
				partition.markDataProduced();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(v1.getProducedDataSets().get(0).getId(), 0));

		assertEquals(4, allocator.getScheduledVertices().size());
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v3.getID(), 0)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v3.getID(), 1)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v4.getID(), 0)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v4.getID(), 1)));

		allocator.clearScheduledVertices();

		when(scheduler.getExecutionJobVertexStatus(v3.getID())).thenReturn(ExecutionState.FINISHED);
		graphManagerPlugin.onExecutionVertexStateChanged(
				new ExecutionVertexStateChangedEvent(new ExecutionVertexID(v3.getID(), 0), ExecutionState.FINISHED));
		assertEquals(2, allocator.getScheduledVertices().size());
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v2.getID(), 0)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v2.getID(), 1)));

		allocator.clearScheduledVertices();

		// Set all blocking partition consumable
		for (int i = 0; i < v4.getParallelism(); i++) {
			for (IntermediateResultPartition partition : eg.getAllVertices().get(v4.getID())
				.getTaskVertices()[i].getProducedPartitions().values()) {
				partition.markFinished();
			}
		}
		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(v4.getProducedDataSets().get(0).getId(), 0));
		assertEquals(2, allocator.getScheduledVertices().size());
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v5.getID(), 0)));
		assertTrue(allocator.getScheduledVertices().contains(new ExecutionVertexID(v5.getID(), 1)));
	}

	/**
	 * Tests that split a group into several ones when resource is not enough.
	 */
	@Test
	public void testSplitGroup() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test group split job", v1, v2, v3, v4);
		jobGraph.getSchedulingConfiguration().setBoolean("job.scheduling.allow-auto-partition", true);

		final SlotProvider slotProvider = new SimpleSlotProvider(jobId, 5);
		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				slotProvider,
				new NoRestartStrategy());

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				new TestExecutionVertexScheduler(eg, Collections.EMPTY_LIST),
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				new TestingExecutionSlotAllocator(eg.getSlotProvider()));

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(1, schedulingGroups.size());

		graphManagerPlugin.onSchedulingStarted();
		waitUntilExecutionVertexState(eg.getJobVertex(v1.getID()).getTaskVertices()[0], ExecutionState.DEPLOYING, 2000L);

		assertEquals(5, graphManagerPlugin.getConcurrentSchedulingGroups().size());
		assertEquals(ResultPartitionType.BLOCKING, eg.getAllVertices().get(v1.getID()).getProducedDataSets()[0].getResultType());
		assertEquals(ResultPartitionType.BLOCKING, eg.getAllVertices().get(v2.getID()).getProducedDataSets()[0].getResultType());
		assertEquals(ResultPartitionType.PIPELINED, eg.getAllVertices().get(v3.getID()).getProducedDataSets()[0].getResultType());
	}

	/**
	 * Tests that resource will be returned to resource manager when a group is cancelled when scheduling.
	 */
	@Test
	public void testResourceWillBeReturnedIfGroupIsCancelled() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		final JobVertex v3 = new JobVertex("vertex3");
		final JobVertex v4 = new JobVertex("vertex4");
		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test group split job", v1, v2, v3, v4);
		jobGraph.getSchedulingConfiguration().setBoolean("job.scheduling.allow-auto-partition", true);

		final BestEffortSlotProvider slotProvider = new BestEffortSlotProvider(jobId, 5);
		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				jobGraph,
				slotProvider,
				new NoRestartStrategy());

		final ConcurrentGroupGraphManagerPlugin graphManagerPlugin = new ConcurrentGroupGraphManagerPlugin();
		graphManagerPlugin.open(
				new TestExecutionVertexScheduler(eg, Collections.EMPTY_LIST),
				jobGraph,
				new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()),
				eg,
				null,
				new TestingExecutionSlotAllocator(eg.getSlotProvider()));

		Set<ConcurrentSchedulingGroup> schedulingGroups = graphManagerPlugin.getConcurrentSchedulingGroups();
		assertEquals(1, schedulingGroups.size());

		graphManagerPlugin.onSchedulingStarted();
		waitUntilExecutionVertexState(eg.getJobVertex(v1.getID()).getTaskVertices()[0], ExecutionState.SCHEDULED, 2000L);
		assertEquals(0, slotProvider.getNumberOfAvailableSlots());

		eg.failGlobal(new Exception("Test scheduling cancelled."));
		waitUntilExecutionVertexState(eg.getJobVertex(v1.getID()).getTaskVertices()[0], ExecutionState.CANCELED, 2000L);

		assertEquals(5, slotProvider.getNumberOfAvailableSlots());
	}

	/**
	 * A testing utility slot provider that return slots if any, or else return a future.
	 */
	private class BestEffortSlotProvider implements SlotProvider, SlotOwner {

		private final Object lock = new Object();

		private final ArrayDeque<SlotContext> slots;

		private final HashMap<SlotRequestId, SlotContext> allocatedSlots;

		public BestEffortSlotProvider(JobID jobId, int numSlots) {
			this(jobId, numSlots, new SimpleAckingTaskManagerGateway());
		}

		public BestEffortSlotProvider(JobID jobId, int numSlots, TaskManagerGateway taskManagerGateway) {
			checkNotNull(jobId, "jobId");
			checkArgument(numSlots >= 0, "numSlots must be >= 0");

			this.slots = new ArrayDeque<>(numSlots);

			for (int i = 0; i < numSlots; i++) {
				SimpleSlotContext as = new SimpleSlotContext(
						new AllocationID(),
						new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + i),
						0,
						taskManagerGateway);
				slots.add(as);
			}

			allocatedSlots = new HashMap<>(slots.size());
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				boolean allowQueued,
				SlotProfile slotProfile,
				Time allocationTimeout) {
			final SlotContext slot;

			synchronized (lock) {
				if (slots.isEmpty()) {
					slot = null;
				} else {
					slot = slots.removeFirst();
				}
				if (slot != null) {
					SimpleSlot result = new SimpleSlot(slot, this, 0);
					allocatedSlots.put(slotRequestId, slot);
					return CompletableFuture.completedFuture(result);
				}
				else {
					return new CompletableFuture<>();
				}
			}
		}

		@Override
		public List<CompletableFuture<LogicalSlot>> allocateSlots(
				List<SlotRequestId> slotRequestIds,
				List<ScheduledUnit> tasks,
				boolean allowQueued,
				List<SlotProfile> slotProfiles,
				Time timeout) {
			List<CompletableFuture<LogicalSlot>> allocationFutures = new ArrayList<>(slotRequestIds.size());
			for (int i = 0; i < slotRequestIds.size(); i++) {
				allocationFutures.add(allocateSlot(slotRequestIds.get(i), tasks.get(i), allowQueued, slotProfiles.get(i), timeout));
			}
			return allocationFutures;
		}

		@Override
		public CompletableFuture<Acknowledge> cancelSlotRequest(
				SlotRequestId slotRequestId,
				@Nullable SlotSharingGroupId slotSharingGroupId,
				@Nullable CoLocationConstraint coLocationConstraint,
				Throwable cause) {
			synchronized (lock) {
				final SlotContext slotContext = allocatedSlots.remove(slotRequestId);

				if (slotContext != null) {
					slots.add(slotContext);
					return CompletableFuture.completedFuture(Acknowledge.get());
				} else {
					return FutureUtils.completedExceptionally(new FlinkException("Unknown slot request id " + slotRequestId + '.'));
				}
			}
		}

		@Override
		public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {
			Preconditions.checkArgument(logicalSlot instanceof Slot);

			final Slot slot = ((Slot) logicalSlot);

			synchronized (lock) {
				slots.add(slot.getSlotContext());
				allocatedSlots.remove(logicalSlot.getSlotRequestId());
			}
			return CompletableFuture.completedFuture(true);
		}

		public int getNumberOfAvailableSlots() {
			synchronized (lock) {
				return slots.size();
			}
		}
	}

	private class TestingExecutionSlotAllocator extends BestEffortExecutionSlotAllocator {

		private Collection<ExecutionVertexID> scheduledVertices = new ArrayList<>();

		TestingExecutionSlotAllocator(SlotProvider slotProvider) {
			super(slotProvider, true, Time.minutes(1));
		}

		@Override
		public CompletableFuture<Collection<LogicalSlot>> allocateSlotsFor(Collection<Execution> executions) {
			for (Execution execution: executions) {
				scheduledVertices.add(execution.getVertex().getExecutionVertexID());
			}
			return super.allocateSlotsFor(executions);
		}

		public Collection<ExecutionVertexID> getScheduledVertices() {
			return scheduledVertices;
		}

		public void clearScheduledVertices() {
			scheduledVertices.clear();
		}
	}
}
