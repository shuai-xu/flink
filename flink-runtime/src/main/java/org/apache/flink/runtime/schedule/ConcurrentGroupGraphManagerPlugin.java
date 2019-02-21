/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.schedule;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobType;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobControlEdge;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SchedulingMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Scheduler plugin which schedules tasks in a concurrent group at the same time.
 */
public class ConcurrentGroupGraphManagerPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(ConcurrentGroupGraphManagerPlugin.class);

	private Set<ConcurrentSchedulingGroup> concurrentSchedulingGroups = new HashSet<>();

	private Map<ExecutionVertexID, Set<ConcurrentSchedulingGroup>> executionToConcurrentSchedulingGroups = new LinkedHashMap<>();

	private Map<JobVertexID, Set<JobVertex>> predecessorToSuccessors = new HashMap<>();

	private Map<JobVertexID, JobVertexID> successorToPredecessors = new HashMap<>();

	private VertexInputTracker inputTracker;

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	@Override
	public void open(VertexScheduler scheduler, JobGraph jobGraph, SchedulingConfig schedulingConfig) {
		this.scheduler = scheduler;
		this.jobGraph = jobGraph;
		this.inputTracker = new VertexInputTracker(jobGraph, scheduler, schedulingConfig);
		buildConcurrentSchedulingGroups();
		buildStartOnFinishRelation(jobGraph);
	}

	private void buildConcurrentSchedulingGroups() {
		List<ConcurrentJobVertexGroup> concurrentJobVertexGroups = new ArrayList<>();
		List<JobVertex> allJobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(allJobVertices));
		} else {
			final Set<JobEdge> visitedJobEdge = new HashSet<>();
			final Set<JobControlEdge> visitedControlEdge = new HashSet<>();

			for (JobVertex jobVertex : allJobVertices) {
				List<JobVertex> concurrentVertices = new ArrayList<>();
				boolean hasConcurrentUpstream = false;
				for (JobEdge input : jobVertex.getInputs()) {
					if (input.getSchedulingMode() == SchedulingMode.CONCURRENT) {
						hasConcurrentUpstream = true;
						break;
					}
				}
				for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
					if (output.getConsumers().size() > 0 &&
							output.getConsumers().get(0).getSchedulingMode() == SchedulingMode.CONCURRENT) {
						hasConcurrentUpstream = true;
						break;
					}
				}
				if (!hasConcurrentUpstream) {
					for (JobControlEdge controlEdge : jobVertex.getInControlEdges()) {
						if (controlEdge.getControlType() == ControlType.CONCURRENT) {
							hasConcurrentUpstream = true;
							break;
						}
					}
				}
				for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
					if (output.getConsumers().size() > 0) {
						JobEdge jobEdge = output.getConsumers().get(0);
						if (!visitedJobEdge.contains(jobEdge) && jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
							visitedJobEdge.add(jobEdge);
							concurrentVertices.add(jobEdge.getTarget());
							concurrentVertices.addAll(getAllConcurrentVertices(jobEdge.getTarget(), visitedJobEdge, visitedControlEdge));
						}
					}
				}
				for (JobControlEdge controlEdge : jobVertex.getOutControlEdges()) {
					if (controlEdge.getControlType() == ControlType.CONCURRENT &&
							!visitedControlEdge.contains(controlEdge)) {
						visitedControlEdge.add(controlEdge);
						concurrentVertices.add(controlEdge.getTarget());
						concurrentVertices.addAll(getAllConcurrentVertices(controlEdge.getTarget(), visitedJobEdge, visitedControlEdge));
					}
				}
				if (!hasConcurrentUpstream || !concurrentVertices.isEmpty()) {
					concurrentVertices.add(jobVertex);
				}
				if (!concurrentVertices.isEmpty()) {
					concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(concurrentVertices));
				}
			}
		}

		LOG.debug("{} vertex group was built for job {}", concurrentJobVertexGroups.size(), jobGraph.getJobID());

		for (ConcurrentJobVertexGroup regionGroup : concurrentJobVertexGroups) {
			Map<ExecutionVertexID, ConcurrentSchedulingGroup> vertexToSchedulingGroups =
					buildSchedulingGroupsFromJobVertexGroup(regionGroup);
			this.concurrentSchedulingGroups.addAll(vertexToSchedulingGroups.values());
			for (Map.Entry<ExecutionVertexID, ConcurrentSchedulingGroup> vertexToGroup: vertexToSchedulingGroups.entrySet()) {
				Set<ConcurrentSchedulingGroup> existingGroups =
						executionToConcurrentSchedulingGroups.computeIfAbsent(vertexToGroup.getKey(), k -> new HashSet<>());
				if (existingGroups != null) {
					existingGroups.add(vertexToGroup.getValue());
				}
			}
		}

		LOG.info("{} concurrent group was built for job {}", concurrentSchedulingGroups.size(), jobGraph.getJobID());
		if (LOG.isDebugEnabled()) {
			for (ConcurrentSchedulingGroup group : concurrentSchedulingGroups) {
				LOG.debug("Concurrent group has {} with preceding {}", group.getExecutionVertices(), group.hasPrecedingGroup());
			}
		}
	}

	@Override
	public void close() {
		// do nothing.
	}

	@Override
	public void reset() {

	}

	@Override
	public void onSchedulingStarted() {
		concurrentSchedulingGroups.stream().forEach(
				(group) -> {
					if (!group.hasPrecedingGroup()) {
						scheduler.scheduleExecutionVertices(group.getExecutionVertices());
					}
		});
	}

	@Override
	public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
		final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();
		final Collection<Collection<ExecutionVertexID>> consumerVertices = jobGraph
				.getResultPartitionConsumerExecutionVertices(event.getResultID(), event.getPartitionNumber());
		for (Collection<ExecutionVertexID> executionVertexIDs : consumerVertices) {
			for (ExecutionVertexID executionVertexID : executionVertexIDs) {
				if (isReadyToSchedule(executionVertexID)) {
					verticesToSchedule.add(executionVertexID);
				}
			}
		}

		scheduleInConcurrentGroup(verticesToSchedule);
	}

	@Override
	public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
		final Set<ConcurrentSchedulingGroup> groupToSchedule = new HashSet<>();

		// For streaming job, region always will be less than concurrent group.
		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			scheduler.scheduleExecutionVertices(event.getAffectedExecutionVertexIDs());
		} else {
			for (ExecutionVertexID executionVertexID : event.getAffectedExecutionVertexIDs()) {
				if (isReadyToSchedule(executionVertexID)) {
					Set<ConcurrentSchedulingGroup> groupsBelongTo = executionToConcurrentSchedulingGroups.get(executionVertexID);
					if (groupsBelongTo.size() == 1) {
						groupToSchedule.add(groupsBelongTo.iterator().next());
					}
				}
			}

			for (ConcurrentSchedulingGroup group : groupToSchedule) {
				scheduler.scheduleExecutionVertices(group.getExecutionVertices());
			}
		}
	}

	@Override
	public synchronized void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {
		final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();
		if (event.getNewExecutionState() == ExecutionState.FINISHED) {
			if (scheduler.getExecutionJobVertexStatus(event.getExecutionVertexID().getJobVertexID()) == ExecutionState.FINISHED) {
				Set<JobVertex> successorVertices = predecessorToSuccessors.get(event.getExecutionVertexID().getJobVertexID());

				if (successorVertices != null) {
					for (JobVertex successor : successorVertices) {
						for (int i = 0; i < successor.getParallelism(); i++) {
							ExecutionVertexID executionVertexID = new ExecutionVertexID(successor.getID(), i);
							if (isReadyToSchedule(executionVertexID)) {
								verticesToSchedule.add(executionVertexID);
							}
						}
					}
				}
			}
		}

		scheduleInConcurrentGroup(verticesToSchedule);
	}

	@Override
	public boolean allowLazyDeployment() {
		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			return false;
		}
		return true;
	}

	@VisibleForTesting
	Set<ConcurrentSchedulingGroup> getConcurrentSchedulingGroups() {
		return concurrentSchedulingGroups;
	}

	private Set<JobVertex> getAllConcurrentVertices(
			JobVertex jobVertex,
			Set<JobEdge> visitedJobEdges,
			Set<JobControlEdge> visitedControlEdges) {
		Set<JobVertex> concurrentVertices = new HashSet<>();
		for (JobEdge jobEdge : jobVertex.getInputs()) {
			if (!visitedJobEdges.contains(jobEdge)) {
				for (JobControlEdge controlEdge :jobEdge.getSource().getProducer().getOutControlEdges()) {
					if (controlEdge.getControlType() == ControlType.START_ON_FINISH) {
						return Collections.emptySet();
					}
				}
				for (JobControlEdge controlEdge :jobEdge.getSource().getProducer().getInControlEdges()) {
					if (controlEdge.getControlType() == ControlType.START_ON_FINISH) {
						return Collections.emptySet();
					}
				}
				if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
					visitedJobEdges.add(jobEdge);
					concurrentVertices.add(jobEdge.getSource().getProducer());
					concurrentVertices.addAll(getAllConcurrentVertices(jobEdge.getSource().getProducer(), visitedJobEdges, visitedControlEdges));
				}
			}
		}
		for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
			if (output.getConsumers().size() > 0) {
				JobEdge jobEdge = output.getConsumers().get(0);
				if (!visitedJobEdges.contains(jobEdge) &&
						jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
					visitedJobEdges.add(jobEdge);
					concurrentVertices.add(jobEdge.getTarget());
					concurrentVertices.addAll(getAllConcurrentVertices(jobEdge.getTarget(), visitedJobEdges, visitedControlEdges));
				}
			}
		}
		for (JobControlEdge controlEdge : jobVertex.getOutControlEdges()) {
			if (!visitedControlEdges.contains(controlEdge) &&
					controlEdge.getControlType() == ControlType.CONCURRENT) {
				visitedControlEdges.add(controlEdge);
				concurrentVertices.add(controlEdge.getTarget());
				concurrentVertices.addAll(getAllConcurrentVertices(controlEdge.getTarget(), visitedJobEdges, visitedControlEdges));
			}
		}
		return concurrentVertices;
	}

	private Map<ExecutionVertexID, ConcurrentSchedulingGroup> buildSchedulingGroupsFromJobVertexGroup(
			ConcurrentJobVertexGroup jobVertexGroup) {

		List<JobVertex> jobVerticesTopologically = jobVertexGroup.getVertices();

		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			return makeAllOneSchedulingGroup(jobVerticesTopologically, jobVertexGroup.hasPrecedingGroup());
		}

		// Make it one region if exists ALL_TO_ALL edge or ControlEdge.
		for (JobVertex jobVertex : jobVerticesTopologically) {
			for (JobControlEdge controlEdge : jobVertex.getOutControlEdges()) {
				if (controlEdge.getControlType() == ControlType.CONCURRENT) {
					return makeAllOneSchedulingGroup(jobVerticesTopologically, jobVertexGroup.hasPrecedingGroup());
				}
			}
			final List<JobEdge> jobEdges = jobVertex.getInputs();
			for (JobEdge jobEdge : jobEdges) {
				if (jobVerticesTopologically.contains(jobEdge.getSource().getProducer()) &&
						jobEdge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL) {
					return makeAllOneSchedulingGroup(jobVerticesTopologically, jobVertexGroup.hasPrecedingGroup());
				}
			}
		}

		final Map<ExecutionVertexID, ConcurrentSchedulingGroup> vertexToSchedulingGroup = new HashMap<>();
		for (JobVertex jobVertex : jobVerticesTopologically) {
			boolean hasUpstream = false;
			for (JobEdge jobEdge : jobVertex.getInputs()) {
				//There will be only one consumer now.
				JobVertex upstreamVertex = jobEdge.getSource().getProducer();
				if (jobVerticesTopologically.contains(upstreamVertex)) {
					hasUpstream = true;
					for (int i = 0; i < upstreamVertex.getParallelism(); i++) {
						Collection<ExecutionVertexID> consumers = jobEdge.getConsumerExecutionVertices(i);
						Set<ExecutionVertexID> verticesInSchedulingGroup = new HashSet<>(1 + consumers.size());
						verticesInSchedulingGroup.add(new ExecutionVertexID(upstreamVertex.getID(), i));
						verticesInSchedulingGroup.addAll(consumers);
						ConcurrentSchedulingGroup schedulingGroup =
								new ConcurrentSchedulingGroup(verticesInSchedulingGroup, jobVertexGroup.hasPrecedingGroup());
						while (!verticesInSchedulingGroup.isEmpty()) {
							Set<ExecutionVertexID> missingVertices = new HashSet<>();
							for (ExecutionVertexID executionVertexID : verticesInSchedulingGroup) {
								ConcurrentSchedulingGroup anotherSchedulingGroup = vertexToSchedulingGroup.get(executionVertexID);
								if (anotherSchedulingGroup != null) {
									missingVertices.addAll(schedulingGroup.merge(anotherSchedulingGroup));
								}
							}
							verticesInSchedulingGroup = missingVertices;
						}
						for (ExecutionVertexID executionVertexID : schedulingGroup.getExecutionVertices()) {
							vertexToSchedulingGroup.put(executionVertexID, schedulingGroup);
						}
					}
				}
			}
			if (!hasUpstream) {
				for (int i = 0; i < jobVertex.getParallelism(); i++) {
					ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertex.getID(), i);
					Set<ExecutionVertexID> verticesInSchedulingGroup = new HashSet<>();
					verticesInSchedulingGroup.add(executionVertexID);
					vertexToSchedulingGroup.put(executionVertexID,
							new ConcurrentSchedulingGroup(verticesInSchedulingGroup, jobVertexGroup.hasPrecedingGroup()));
				}
			}
		}
		return vertexToSchedulingGroup;
	}

	private Map<ExecutionVertexID, ConcurrentSchedulingGroup> makeAllOneSchedulingGroup(
			Iterable<JobVertex> jobVertices, boolean hasPrecedingGroup) {

		final Map<ExecutionVertexID, ConcurrentSchedulingGroup> vertexToSchedulingGroup = new HashMap<>();
		final Set<ExecutionVertexID> allVertices = new HashSet<>();

		for (JobVertex jobVertex : jobVertices) {
			for (int i = 0; i < jobVertex.getParallelism(); i++) {
				allVertices.add(new ExecutionVertexID(jobVertex.getID(), i));
			}
		}

		final ConcurrentSchedulingGroup singleGroup = new ConcurrentSchedulingGroup(allVertices, hasPrecedingGroup);

		for (ExecutionVertexID executionVertexID : singleGroup.getExecutionVertices()) {
			vertexToSchedulingGroup.put(executionVertexID, singleGroup);
		}
		return vertexToSchedulingGroup;
	}

	private void scheduleInConcurrentGroup(Set<ExecutionVertexID> verticesToSchedule) {
		Set<ConcurrentSchedulingGroup> groupsToSchedule = new HashSet<>();
		for (ExecutionVertexID vertexID : verticesToSchedule) {
			Set<ConcurrentSchedulingGroup> groupsBelongTo = executionToConcurrentSchedulingGroups.get(vertexID);
			if (groupsBelongTo == null) {
				throw new RuntimeException("Can not find a group for " + vertexID + ", this is logic error.");
			}
			if (groupsBelongTo.size() <= 1) {
				groupsToSchedule.addAll(groupsBelongTo);
			}
		}
		for (ConcurrentSchedulingGroup group : groupsToSchedule) {
			scheduler.scheduleExecutionVertices(group.getExecutionVertices());
		}
	}

	private boolean isReadyToSchedule(ExecutionVertexID vertexID) {
		ExecutionVertexStatus vertexStatus = scheduler.getExecutionVertexStatus(vertexID);

		// only CREATED vertices can be scheduled
		if (vertexStatus.getExecutionState() != ExecutionState.CREATED) {
			return false;
		}

		JobVertexID predecessorId = successorToPredecessors.get(vertexID.getJobVertexID());
		if (predecessorId != null && scheduler.getExecutionJobVertexStatus(predecessorId) != ExecutionState.FINISHED) {
			return false;
		}

		// source vertices can be scheduled at once
		if (jobGraph.findVertexByID(vertexID.getJobVertexID()).isInputVertex()) {
			return true;
		}

		// query whether the inputs are ready overall
		return inputTracker.areInputsReady(vertexID);
	}

	private void buildStartOnFinishRelation(JobGraph jobGraph) {
		successorToPredecessors.clear();
		predecessorToSuccessors.clear();

		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			for (JobControlEdge controlEdge : jobVertex.getOutControlEdges()) {
				LOG.debug("ControlEdge from {} to {} with type {}",
						controlEdge.getSource().getID(), controlEdge.getTarget().getID(), controlEdge.getControlType());
				if (controlEdge.getControlType() == ControlType.START_ON_FINISH) {
					Set<JobVertex> concurrentAncestors = getAllConcurrentAncestors(controlEdge.getTarget());
					for (JobVertex ancestor : concurrentAncestors) {
						successorToPredecessors.put(ancestor.getID(), jobVertex.getID());
					}
					Set<JobVertex> existingSuccessors = predecessorToSuccessors.putIfAbsent(jobVertex.getID(), concurrentAncestors);
					if (existingSuccessors != null) {
						existingSuccessors.addAll(concurrentAncestors);
					}
				}
			}
		}
	}

	private static Set<JobVertex> getAllConcurrentAncestors(JobVertex jobVertex) {
		if (jobVertex.isInputVertex()) {
			return Collections.singleton(jobVertex);
		} else {
			Set<JobVertex> ancestors = new HashSet<>();
			for (JobEdge jobEdge : jobVertex.getInputs()) {
				if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT) {
					ancestors.addAll(getAllConcurrentAncestors(jobEdge.getSource().getProducer()));
				}
			}
			if (ancestors.isEmpty()) {
				ancestors.add(jobVertex);
			}
			return ancestors;
		}
	}

}
