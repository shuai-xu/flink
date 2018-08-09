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

import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the default graph manager plugin.
 * It decides which execution vertices to schedule according to its {@link ScheduleMode}.
 */
public class DefaultGraphManagerPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultGraphManagerPlugin.class);

	private ScheduleMode scheduleMode = ScheduleMode.LAZY_FROM_SOURCES;

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	@Override
	public void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig config) {
		checkNotNull(jobGraph);
		checkNotNull(config);

		this.scheduler = scheduler;
		this.jobGraph = jobGraph;
		this.scheduleMode = ScheduleMode.valueOf(
			config.getConfiguration().getString(ScheduleMode.class.getName(), ScheduleMode.LAZY_FROM_SOURCES.toString()));

		LOG.info("DefaultGraphManagerPlugin opened with schedule mode: {}.", scheduleMode);
	}

	@Override
	public void close() {

	}

	@Override
	public void reset() {

	}

	@Override
	public void onSchedulingStarted() {
		final List<ExecutionVertexID> verticesToSchedule = new ArrayList<>();
		switch (scheduleMode) {
			case EAGER:
				for (JobVertex vertex : jobGraph.getVertices()) {
					for (int i = 0; i < vertex.getParallelism(); i++) {
						verticesToSchedule.add(new ExecutionVertexID(vertex.getID(), i));
					}
				}
				scheduler.scheduleExecutionVertices(verticesToSchedule);
				break;
			case LAZY_FROM_SOURCES:
				for (JobVertex vertex : jobGraph.getVertices()) {
					if (vertex.getInputs().size() == 0) {
						for (int i = 0; i < vertex.getParallelism(); i++) {
							verticesToSchedule.add(new ExecutionVertexID(vertex.getID(), i));
						}
					}
				}
				scheduleOneByOne(verticesToSchedule);
				break;
			default:
				throw new IllegalArgumentException("Invalid schedule mode: " + scheduleMode);
		}
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		switch (scheduleMode) {
			case EAGER:
				throw new IllegalStateException("No input data consumable notification should happen in EAGER mode.");
			case LAZY_FROM_SOURCES:
				final List<ExecutionVertexID> verticesToSchedule = new ArrayList<>();
				final Collection<Collection<ExecutionVertexID>> consumerVertices = jobGraph
					.getResultPartitionConsumerExecutionVertices(event.getResultID(), event.getPartitionNumber());
				for (Collection<ExecutionVertexID> executionVertexIDs : consumerVertices) {
					for (ExecutionVertexID executionVertexID : executionVertexIDs) {
						if (scheduler.getExecutionVertexStatus(executionVertexID).getExecutionState() == ExecutionState.CREATED) {
							verticesToSchedule.add(executionVertexID);
						}
					}
				}
				scheduleOneByOne(verticesToSchedule);
				break;
			default:
				throw new IllegalArgumentException("Invalid schedule mode: " + scheduleMode);
		}
	}

	@Override
	public void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {

	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		switch (scheduleMode) {
			case EAGER:
				scheduler.scheduleExecutionVertices(event.getAffectedExecutionVertexIDs());
				break;
			case LAZY_FROM_SOURCES:
				final List<ExecutionVertexID> verticesToRestartNow = new ArrayList<>();
				for (ExecutionVertexID executionVertexID : event.getAffectedExecutionVertexIDs()) {
					if (scheduler.getExecutionVertexStatus(executionVertexID).isInputDataConsumable()) {
						verticesToRestartNow.add(executionVertexID);
					}
				}
				scheduleOneByOne(verticesToRestartNow);
				break;
			default:
				throw new IllegalArgumentException("Invalid schedule mode: " + scheduleMode);
		}
	}

	@Override
	public boolean allowLazyDeployment() {
		switch (scheduleMode) {
			case EAGER:
				return false;
			case LAZY_FROM_SOURCES:
				return true;
			default:
				throw new IllegalArgumentException("Invalid schedule mode: " + scheduleMode);
		}
	}

	/**
	 * Schedule execution vertices one by one. These vertices will allocate resource and get to deploy individually.
	 */
	private void scheduleOneByOne(final List<ExecutionVertexID> verticesToSchedule) {
		for (ExecutionVertexID executionVertexID : verticesToSchedule) {
			scheduler.scheduleExecutionVertices(Collections.singleton(executionVertexID));
		}
	}
}
