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
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StepwiseSchedulingPlugin}.
 */
public class StepwiseSchedulingPluginTest extends GraphManagerPluginTestBase {

	/**
	 * Tests stepwise scheduling.
	 */
	@Test
	public void testStepwiseScheduling() throws Exception {

		final JobID jobId = new JobID();
		final JobVertex v1 = new JobVertex("vertex1");
		final JobVertex v2 = new JobVertex("vertex2");
		v1.setParallelism(3);
		v2.setParallelism(4);
		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobGraph jobGraph = new JobGraph(jobId, "test job", new JobVertex[] {v1, v2});
		jobGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			jobGraph,
			new SimpleAckingTaskManagerGateway(),
			new NoRestartStrategy());

		final List<ExecutionVertex> executionVertices = new ArrayList<>();
		final List<ExecutionVertexID> vertices = new ArrayList<>();
		for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
			for (ExecutionVertex ev : ejv.getTaskVertices()) {
				executionVertices.add(ev);
				vertices.add(ev.getExecutionVertexID());
			}
		}

		final Iterator<ExecutionJobVertex> ejvIterator = eg.getVerticesTopologically().iterator();
		final ExecutionJobVertex ejv1 = ejvIterator.next();
		final List<ExecutionVertexID> vertices1 = new ArrayList<>();
		for (ExecutionVertex ev : ejv1.getTaskVertices()) {
			vertices1.add(ev.getExecutionVertexID());
		}

		final ExecutionJobVertex ejv2 = ejvIterator.next();
		final List<ExecutionVertexID> vertices2 = new ArrayList<>();
		for (ExecutionVertex ev : ejv2.getTaskVertices()) {
			vertices2.add(ev.getExecutionVertexID());
		}

		final TestExecutionVertexScheduler scheduler = new TestExecutionVertexScheduler(executionVertices);

		final GraphManagerPlugin graphManagerPlugin = new StepwiseSchedulingPlugin();
		graphManagerPlugin.open(
			scheduler,
			jobGraph,
			new SchedulingConfig(jobGraph.getSchedulingConfiguration(), this.getClass().getClassLoader()));

		graphManagerPlugin.onSchedulingStarted();
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices1));
		scheduler.clearScheduledVertices();

		graphManagerPlugin.onResultPartitionConsumable(
			new ResultPartitionConsumableEvent(ejv1.getProducedDataSets()[0].getId(), 0));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices2));
		scheduler.clearScheduledVertices();

		graphManagerPlugin.onExecutionVertexFailover(new ExecutionVertexFailoverEvent(vertices));
		assertTrue(compareVertices(scheduler.getScheduledVertices(), vertices1));
	}
}
