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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.ControlType;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobControlEdge;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SchedulingMode;
import org.apache.flink.runtime.jobmaster.ExecutionSlotAllocator;
import org.apache.flink.runtime.jobmaster.GraphManager;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * A Scheduler plugin which schedules tasks in a concurrent group at the same time.
 */
public class ConcurrentGroupGraphManagerPlugin implements GraphManagerPlugin {

	private static final Logger LOG = LoggerFactory.getLogger(ConcurrentGroupGraphManagerPlugin.class);

	private Set<ConcurrentSchedulingGroup> concurrentSchedulingGroups = new HashSet<>();

	private Map<ExecutionVertexID, ConcurrentSchedulingGroup> executionToConcurrentSchedulingGroups = new LinkedHashMap<>();

	private Map<JobVertexID, Set<JobVertex>> predecessorToSuccessors = new HashMap<>();

	private Map<JobVertexID, Set<JobVertexID>> successorToPredecessors = new HashMap<>();

	private Set<JobControlEdge> ignoredControlEdges = new HashSet<>();

	private VertexInputTracker inputTracker;

	private VertexScheduler scheduler;

	private JobGraph jobGraph;

	private ExecutionGraph executionGraph;

	private GraphManager graphManager;

	private ExecutionSlotAllocator executionSlotAllocator;

	private boolean allowGroupSplit;

	private Time allocationLongTimeout;
	@Override
	public void open(
			VertexScheduler scheduler,
			JobGraph jobGraph,
			SchedulingConfig schedulingConfig,
			ExecutionGraph eg,
			GraphManager graphManager,
			ExecutionSlotAllocator executionSlotAllocator) {
		this.scheduler = scheduler;
		this.jobGraph = jobGraph;
		this.inputTracker = new VertexInputTracker(jobGraph, scheduler, schedulingConfig);
		this.executionGraph = eg;
		this.graphManager = graphManager;
		this.allowGroupSplit = schedulingConfig.getConfiguration().getBoolean(JobManagerOptions.ALLOW_GROUP_SPLIT);
		this.executionSlotAllocator = executionSlotAllocator;
		this.allocationLongTimeout = Time.milliseconds(schedulingConfig.getConfiguration().getLong(JobManagerOptions.SLOT_REQUEST_LONG_TIMEOUT));
		initConcurrentSchedulingGroups();
	}

	private void initConcurrentSchedulingGroups() {
		List<ConcurrentJobVertexGroup> concurrentJobVertexGroups = new ArrayList<>();
		List<JobVertex> allJobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		if (jobGraph.getJobType() == JobType.INFINITE_STREAM) {
			LOG.debug("All executions will be in one group for streaming job {}", jobGraph.getJobID());

			List<ExecutionVertex> allExecutionVertices = new ArrayList<>(executionGraph.getRegisteredExecutions().size());
			for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
				allExecutionVertices.add(ev);
			}
			concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(allJobVertices, ignoredControlEdges));
			this.concurrentSchedulingGroups.add(
					new ConcurrentSchedulingGroup(allExecutionVertices, false));
		} else {
			buildStartOnFinishRelation(jobGraph);
			buildConcurrentSchedulingGroups(allJobVertices);
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
		// To avoid concurrent modification as the groups may change due to group split.
		List<ConcurrentSchedulingGroup> groups = new ArrayList<>(concurrentSchedulingGroups);
		groups.stream().forEach(
				(group) -> {
					if (!group.hasPrecedingGroup()) {
						scheduleGroup(group);
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
					ConcurrentSchedulingGroup groupsBelongTo = executionToConcurrentSchedulingGroups.get(executionVertexID);
					groupToSchedule.add(groupsBelongTo);
				}
			}

			for (ConcurrentSchedulingGroup group : groupToSchedule) {
				if (!graphManager.cacheGroupIfReconciling(group)) {
					scheduleGroup(group);
				}
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

	private List<ConcurrentSchedulingGroup> buildConcurrentSchedulingGroups(List<JobVertex> jobVerticesTopologically) {
		List<ConcurrentJobVertexGroup> concurrentJobVertexGroups = new ArrayList<>();
		List<ConcurrentSchedulingGroup> schedulingGroups = new ArrayList<>();

		final Set<JobVertex> visitedJobVertices = new HashSet<>();

		for (JobVertex jobVertex : jobVerticesTopologically) {
			if (visitedJobVertices.add(jobVertex)) {

				List<JobVertex> concurrentVertices = new ArrayList<>();
				concurrentVertices.add(jobVertex);

				for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
					for (JobEdge jobEdge : output.getConsumers()) {
						if (jobVerticesTopologically.contains(jobEdge.getTarget()) &&
								!visitedJobVertices.contains(jobEdge.getTarget()) &&
								jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT &&
								!isStartOnFinishedEdge(jobEdge)) {
							visitedJobVertices.add(jobEdge.getTarget());
							concurrentVertices.add(jobEdge.getTarget());
							concurrentVertices.addAll(
									getAllConcurrentVertices(jobEdge.getTarget(), jobVerticesTopologically, visitedJobVertices));
						}
					}
				}

				concurrentJobVertexGroups.add(new ConcurrentJobVertexGroup(concurrentVertices, ignoredControlEdges));
			}
		}

		LOG.info("{} vertex group was built with {} vertices.", concurrentJobVertexGroups.size(), jobVerticesTopologically.size());

		breakCircleDependencies(concurrentJobVertexGroups);

		for (ConcurrentJobVertexGroup group : concurrentJobVertexGroups) {
			LOG.info("Concurrent vertex group has {} with preceding {}", group.getVertices(), group.hasPrecedingGroup());
		}

		for (ConcurrentJobVertexGroup regionGroup : concurrentJobVertexGroups) {
			schedulingGroups.addAll(buildSchedulingGroupsFromJobVertexGroup(regionGroup));
		}

		this.concurrentSchedulingGroups.addAll(schedulingGroups);
		for (ConcurrentSchedulingGroup schedulingGroup: schedulingGroups) {
			for (ExecutionVertex ev : schedulingGroup.getExecutionVertices()) {
				executionToConcurrentSchedulingGroups.put(ev.getExecutionVertexID(), schedulingGroup);
			}
		}

		LOG.info("{} concurrent group was built with {} vertices for job {}.",
				schedulingGroups.size(), jobVerticesTopologically.size(), jobGraph.getJobID());

		return schedulingGroups;
	}

	private void splitGroupAndContinueScheduling(
			List<JobVertex> assignedJobVertices,
			List<JobVertex> unAssignedJobVertices,
			ConcurrentSchedulingGroup originalGroup) {
		LOG.info("Split scheduling group {} as resource is not enough, assigned {}, unassigned {}.",
				originalGroup, assignedJobVertices, unAssignedJobVertices);

		concurrentSchedulingGroups.remove(originalGroup);

		// 1. Update the result partition.
		Set<JobVertex> visitedJobVertices = new HashSet<>();
		for (JobVertex jobVertex : assignedJobVertices) {
			if (visitedJobVertices.add(jobVertex)) {
				for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
					IntermediateDataSet output = jobVertex.getProducedDataSets().get(i);
					if (!output.getConsumers().isEmpty()) {
						JobEdge jobEdge = output.getConsumers().get(0);
						for (ExecutionVertex executionVertex : executionGraph.getJobVertex(jobEdge.getTarget().getID()).getTaskVertices()) {
							if (executionVertex.getExecutionState() == ExecutionState.CREATED) {
								jobEdge.setSchedulingMode(SchedulingMode.SEQUENTIAL);
								executionGraph.getJobVertex(jobVertex.getID()).getProducedDataSets()[i].setResultType(ResultPartitionType.BLOCKING);
								break;
							}
						}
					}
				}
			}
		}
		// 2. Rebuild virtual relations.
		buildStartOnFinishRelation(jobGraph);
		// 3. Build groups for the job vertices that have been assigned resource.
		List<ConcurrentSchedulingGroup> newAssignedGroups = buildConcurrentSchedulingGroups(assignedJobVertices);
		// 4. Build groups for the job vertices that have not been assigned resource.
		List<ConcurrentSchedulingGroup> newUnAssignedGroups = buildConcurrentSchedulingGroups(unAssignedJobVertices);
		// 5. Rebuild failover region.
		// 6. Deploy the tasks.
		for (ConcurrentSchedulingGroup group : newAssignedGroups) {
			List<ExecutionVertex> evs = group.getExecutionVertices();
			if (evs.size() == 1 && evs.get(0).getCurrentAssignedResource() == null) {
				scheduleGroup(group);
			} else {
				for (ExecutionVertex ev : evs) {
					try {
						ev.getCurrentExecutionAttempt().deploy();
					} catch (Exception e) {
						LOG.info("Fail to deploy execution {}", ev, e);
						ev.getCurrentExecutionAttempt().fail(e);
					}
				}
			}
		}
		// 7. Trigger groups that have no preceding
		for (ConcurrentSchedulingGroup group : newUnAssignedGroups) {
			if (!group.hasPrecedingGroup()) {
				scheduleGroup(group);
			} else {
				List<ExecutionVertex> evs = group.getExecutionVertices();
				for (ExecutionVertex ev : evs) {
					if (isReadyToSchedule(ev.getExecutionVertexID())) {
						scheduleGroup(group);
						break;
					}
				}
			}
		}
	}

	@VisibleForTesting
	Set<ConcurrentSchedulingGroup> getConcurrentSchedulingGroups() {
		return concurrentSchedulingGroups;
	}

	@VisibleForTesting
	Map<JobVertexID, Set<JobVertex>> getPredecessorToSuccessors() {
		return predecessorToSuccessors;
	}

	@VisibleForTesting
	Map<JobVertexID, Set<JobVertexID>> getSuccessorsToPredecessor() {
		return successorToPredecessors;
	}

	private Set<JobVertex> getAllConcurrentVertices(
			JobVertex jobVertex,
			List<JobVertex> allJobVerticesTopologically,
			Set<JobVertex> visitedJobVertices) {
		Set<JobVertex> concurrentVertices = new HashSet<>();

		for (JobEdge jobEdge : jobVertex.getInputs()) {
			if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT &&
					allJobVerticesTopologically.contains(jobEdge.getSource().getProducer()) &&
					!visitedJobVertices.contains(jobEdge.getSource().getProducer()) &&
					!isStartOnFinishedEdge(jobEdge)) {
				visitedJobVertices.add(jobEdge.getSource().getProducer());
				concurrentVertices.add(jobEdge.getSource().getProducer());
				concurrentVertices.addAll(getAllConcurrentVertices(
							jobEdge.getSource().getProducer(), allJobVerticesTopologically, visitedJobVertices));
			}
		}
		for (IntermediateDataSet output : jobVertex.getProducedDataSets()) {
			for (JobEdge jobEdge : output.getConsumers()) {
				if (allJobVerticesTopologically.contains(jobEdge.getTarget()) &&
						!visitedJobVertices.contains(jobEdge.getTarget()) &&
						jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT &&
						!isStartOnFinishedEdge(jobEdge)) {
					visitedJobVertices.add(jobEdge.getTarget());
					concurrentVertices.add(jobEdge.getTarget());
					concurrentVertices.addAll(
							getAllConcurrentVertices(jobEdge.getTarget(), allJobVerticesTopologically, visitedJobVertices));
				}
			}
		}
		return concurrentVertices;
	}

	private List<ConcurrentSchedulingGroup> buildSchedulingGroupsFromJobVertexGroup(
			ConcurrentJobVertexGroup jobVertexGroup) {

		final List<ConcurrentSchedulingGroup> schedulingGroups = new ArrayList<>();

		List<JobVertex> jobVerticesTopologically = jobVertexGroup.getVertices();
		if (jobVerticesTopologically.size() == 1) {
			for (ExecutionVertex ev : executionGraph.getJobVertex(jobVerticesTopologically.get(0).getID()).getTaskVertices()) {
				schedulingGroups.add(
						new ConcurrentSchedulingGroup(
								Collections.singletonList(ev),
								jobVertexGroup.hasPrecedingGroup()));
			}
		} else {
			List<ExecutionVertex> executionVertices = new ArrayList<>();
			for (JobVertex jobVertex : jobVerticesTopologically) {
				for (ExecutionVertex ev : executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices()) {
					executionVertices.add(ev);
				}
			}
			schedulingGroups.add(new ConcurrentSchedulingGroup(
					executionVertices,
					jobVertexGroup.hasPrecedingGroup()));
		}

		return schedulingGroups;
	}

	private void scheduleInConcurrentGroup(Set<ExecutionVertexID> verticesToSchedule) {
		Set<ConcurrentSchedulingGroup> groupsToSchedule = new HashSet<>();
		for (ExecutionVertexID vertexID : verticesToSchedule) {
			ConcurrentSchedulingGroup groupsBelongTo = executionToConcurrentSchedulingGroups.get(vertexID);
			if (groupsBelongTo == null) {
				throw new RuntimeException("Can not find a group for " + vertexID + ", this is logic error.");
			}
			groupsToSchedule.add(groupsBelongTo);
		}
		for (ConcurrentSchedulingGroup group : groupsToSchedule) {
			if (!graphManager.cacheGroupIfReconciling(group)) {
				scheduleGroup(group);
			}
		}
	}

	private boolean isReadyToSchedule(ExecutionVertexID vertexID) {
		ExecutionVertexStatus vertexStatus = scheduler.getExecutionVertexStatus(vertexID);

		// only CREATED vertices can be scheduled
		if (vertexStatus.getExecutionState() != ExecutionState.CREATED) {
			return false;
		}

		Set<JobVertexID> predecessorIds = successorToPredecessors.get(vertexID.getJobVertexID());
		if (predecessorIds != null) {
			for (JobVertexID predecessorId : predecessorIds) {
				if (scheduler.getExecutionJobVertexStatus(predecessorId) != ExecutionState.FINISHED) {
					return false;
				}
			}
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
					boolean hasCircleDependency = false;
					for (JobVertex ancestor : concurrentAncestors) {
						if (hasCircleDependencyInVertices(ancestor, jobVertex)) {
							hasCircleDependency = true;
							ignoredControlEdges.add(controlEdge);
							break;
						}
					}
					if (!hasCircleDependency) {
						for (JobVertex ancestor : concurrentAncestors) {
							Set<JobVertexID> existingPredecessors = successorToPredecessors.computeIfAbsent(ancestor.getID(), k -> new HashSet<>());
							existingPredecessors.add(jobVertex.getID());
						}

						Set<JobVertex> existingSuccessors = predecessorToSuccessors.putIfAbsent(jobVertex.getID(), concurrentAncestors);
						if (existingSuccessors != null) {
							existingSuccessors.addAll(concurrentAncestors);
						}
					}
				}
			}
		}
	}

	private Set<JobVertex> getAllConcurrentAncestors(JobVertex jobVertex) {
		Set<JobVertex> ancestors = new HashSet<>();
		if (jobVertex.isInputVertex()) {
			ancestors.add(jobVertex);
		} else {
			for (JobEdge jobEdge : jobVertex.getInputs()) {
				if (jobEdge.getSchedulingMode() == SchedulingMode.CONCURRENT &&
						!isStartOnFinishedEdge(jobEdge)) {
					ancestors.addAll(getAllConcurrentAncestors(jobEdge.getSource().getProducer()));
				}
			}
			if (ancestors.isEmpty()) {
				ancestors.add(jobVertex);
			}
		}
		return ancestors;
	}

	public void scheduleGroup(ConcurrentSchedulingGroup schedulingGroup) {

		List<ExecutionVertex> executionVertices = schedulingGroup.getExecutionVertices();
		List<Execution> scheduledExecutions = new ArrayList<>();

		for (ExecutionVertex ev : executionVertices) {
			if (ev.getExecutionState() == ExecutionState.CREATED) {
				scheduledExecutions.add(ev.getCurrentExecutionAttempt());
			}
		}
		Time allocationTimeout = executionVertices.size() > 1 ? executionGraph.getAllocationTimeout() : allocationLongTimeout;
		CompletableFuture<Collection<LogicalSlot>> allocationFuture =
				executionSlotAllocator.allocateSlotsFor(scheduledExecutions, allocationTimeout);
		CompletableFuture<Void> currentSchedulingFuture = allocationFuture.handleAsync(
				(Collection<LogicalSlot> slots, Throwable throwable) -> {
					if (throwable == null) {
						int failedNumber = 0;
						for (LogicalSlot slot : slots) {
							if (slot == null) {
								failedNumber++;
							}
						}
						Throwable strippedThrowable = new Exception("Batch request " + scheduledExecutions.size() +
								", but " + failedNumber + " does not return.");
						if (failedNumber > 0 && (!allowGroupSplit || executionVertices.size() < 2)) {
							for (LogicalSlot slot : slots) {
								if (slot != null) {
									slot.releaseSlot(strippedThrowable);
								}
							}
							for (Execution execution : scheduledExecutions) {
								execution.fail(strippedThrowable);
							}
							return null;
						} else if (failedNumber > 0) {
							int i = 0;
							int index = -1;
							boolean hasFailure = false;
							// Find the index from which resource is not assigned.
							for (LogicalSlot slot : slots) {
								if (slot == null && !hasFailure) {
									hasFailure = true;
									index = i;
									scheduledExecutions.get(i).rollbackToCreated();
									LOG.debug("The first failed slot request is {}.", index);
								} else if (hasFailure) {
									if (slot != null) {
										slot.releaseSlot(strippedThrowable);
									}
									scheduledExecutions.get(i).rollbackToCreated();
								}
								i++;
							}
							if (index < 0) {
								LOG.info("All allocations is assigned, but the request fail, this is strange.", throwable);
							} else {
								List<JobVertex> assignedJobVertices = new ArrayList<>();
								List<JobVertex> unAssignedJobVertices = new ArrayList<>();
								if (index == 0) {
									assignedJobVertices.add(executionVertices.get(0).getJobVertex().getJobVertex());
									for (int j = 1; j < executionVertices.size(); j++) {
										JobVertexID jobVertexID = executionVertices.get(j).getJobvertexId();
										if (!jobVertexID.equals(assignedJobVertices.get(assignedJobVertices.size() - 1).getID())
												&& (unAssignedJobVertices.isEmpty() ||
												!jobVertexID.equals(unAssignedJobVertices.get(unAssignedJobVertices.size() - 1).getID()))) {
											unAssignedJobVertices.add(executionVertices.get(j).getJobVertex().getJobVertex());
										}
									}
								} else {
									boolean lastAssignedVertexFulfilled = false;
									boolean firstVertexFullyAssigned = true;
									if (!scheduledExecutions.get(index).getVertex().getJobvertexId().equals(
											scheduledExecutions.get(index - 1).getVertex().getJobvertexId())) {
										lastAssignedVertexFulfilled = true;
										LOG.debug("The last assigned vertex is fully filled.");
									}
									if (scheduledExecutions.get(index).getVertex().getJobvertexId().equals(
											executionVertices.get(0).getJobvertexId())) {
										firstVertexFullyAssigned = false;
										LOG.debug("The first vertex is not fully filled.");
									}
									i = 0;
									for (LogicalSlot slot : slots) {
										if (lastAssignedVertexFulfilled) {
											assignResourceElseFail(scheduledExecutions.get(i), slot);
										} else {
											if (firstVertexFullyAssigned) {
												if (executionVertices.get(i).getJobvertexId().equals(
														scheduledExecutions.get(index).getVertex().getJobvertexId())) {
													slot.releaseSlot(strippedThrowable);
													scheduledExecutions.get(i).rollbackToCreated();
												} else {
													assignResourceElseFail(scheduledExecutions.get(i), slot);
												}
											} else {
												assignResourceElseFail(scheduledExecutions.get(i), slot);
											}
										}
										i++;
										if (i >= index) {
											break;
										}
									}
									assignedJobVertices.add(executionVertices.get(0).getJobVertex().getJobVertex());
									boolean enterUnAssigned = false;
									for (int j = 0; j < executionVertices.size(); j++) {
										JobVertex jobVertex = executionVertices.get(j).getJobVertex().getJobVertex();
										if (!enterUnAssigned) {
											if (jobVertex.getID().equals(
													scheduledExecutions.get(index).getVertex().getJobvertexId())) {
												if (!jobVertex.getID().equals(assignedJobVertices.get(0).getID())) {
													unAssignedJobVertices.add(jobVertex);
												}
												enterUnAssigned = true;
											} else if (!jobVertex.getID().equals(
													assignedJobVertices.get(assignedJobVertices.size() - 1).getID())) {
												assignedJobVertices.add(jobVertex);
											}
										} else if (!jobVertex.getID().equals(assignedJobVertices.get(0).getID()) &&
												(unAssignedJobVertices.isEmpty() || !jobVertex.getID().equals(
														unAssignedJobVertices.get(unAssignedJobVertices.size() - 1).getID()))) {
											unAssignedJobVertices.add(jobVertex);
										}
									}
								}
								splitGroupAndContinueScheduling(assignedJobVertices, unAssignedJobVertices, schedulingGroup);
								return null;
							}
						}
						int i = 0;
						for (LogicalSlot slot : slots) {
							if (!scheduledExecutions.get(i).tryAssignResource(slot)) {
								// release the slot
								Exception e = new FlinkException("Could not assign logical slot to execution " + scheduledExecutions.get(i) + '.');
								slot.releaseSlot(e);
								scheduledExecutions.get(i).fail(e);
							}
							i++;
						}
						for (i = 0; i <  scheduledExecutions.size(); i++) {
							try {
								scheduledExecutions.get(i).deploy();
							} catch (Exception e) {
								LOG.info("Fail to deploy execution {}", scheduledExecutions.get(i), e);
								scheduledExecutions.get(i).fail(e);
							}
						}
					}
					return null;
				}, executionGraph.getFutureExecutor());

		executionGraph.registerSchedulingFuture(currentSchedulingFuture);

		currentSchedulingFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
					if (strippedThrowable instanceof CancellationException) {
						// cancel the individual allocation futures
						allocationFuture.cancel(false);
					}
					executionGraph.unregisterSchedulingFuture(currentSchedulingFuture);
				});
	}

	private void assignResourceElseFail(Execution execution, LogicalSlot slot) {

		if (!execution.tryAssignResource(slot)) {
			// release the slot
			Exception e = new FlinkException("Could not assign logical slot to execution " + execution + '.');
			slot.releaseSlot(e);
			execution.fail(e);
		}
	}

	private void breakCircleDependencies(List<ConcurrentJobVertexGroup> jobVertexGroups) {
		Map<JobVertex, ConcurrentJobVertexGroup> vertexToConcurrentGroupMap = new HashMap<>();
		for (ConcurrentJobVertexGroup jobVertexGroup : jobVertexGroups) {
			List<JobVertex> vertices = jobVertexGroup.getVertices();
			for (JobVertex vertex : vertices) {
				vertexToConcurrentGroupMap.put(vertex, jobVertexGroup);
			}
		}
		for (ConcurrentJobVertexGroup jobVertexGroup : jobVertexGroups) {
			if (hasCircleDependencyInGroups(jobVertexGroup, vertexToConcurrentGroupMap) &&
					jobVertexGroup.hasInputVertex()) {
				jobVertexGroup.noPrecedingGroup();
			}
		}
	}

	private boolean hasCircleDependencyInGroups(
			ConcurrentJobVertexGroup jobVertexGroup,
			Map<JobVertex, ConcurrentJobVertexGroup> vertexToConcurrentGroupMap) {

		List<JobVertex> predecessors = jobVertexGroup.getPredecessorVertices();
		if (predecessors.size() > 0) {
			List<JobVertex> ancestors = new ArrayList<>();
			Set<ConcurrentJobVertexGroup> visitedGroups = new HashSet<>();
			for (JobVertex predecessor : predecessors) {
				ConcurrentJobVertexGroup group = vertexToConcurrentGroupMap.get(predecessor);
				if (group != null) {
					ancestors.addAll(group.getPredecessorVertices());
					if (!visitedGroups.contains(group)) {
						visitedGroups.add(group);
					}
				}
			}
			while (!ancestors.isEmpty()) {
				List<JobVertex> newAddedAncestors = new ArrayList<>();
				for (JobVertex ancestor : ancestors) {
					ConcurrentJobVertexGroup group = vertexToConcurrentGroupMap.get(ancestor);
					if (group == jobVertexGroup) {
						return true;
					} else {
						if (!visitedGroups.contains(group)) {
							visitedGroups.add(group);
							newAddedAncestors.addAll(group.getPredecessorVertices());
						}
					}
				}
				ancestors = newAddedAncestors;
			}
		}
		return false;
	}

	private boolean hasCircleDependencyInVertices(JobVertex successor, JobVertex predecessor) {
		List<JobVertex> ancestors = new ArrayList<>();
		Set<JobVertexID> virtualPredecessors = successorToPredecessors.get(predecessor.getID());
		if (virtualPredecessors != null) {
			for (JobVertexID virtualPredecessor : virtualPredecessors) {
				ancestors.add(executionGraph.getJobVertex(virtualPredecessor).getJobVertex());
			}
		}
		for (JobEdge jobEdge : predecessor.getInputs()) {
			ancestors.add(jobEdge.getSource().getProducer());
		}
		while (!ancestors.isEmpty()) {
			List<JobVertex> newAddedAncestors = new ArrayList<>();
			for (JobVertex ancestor : ancestors) {
				if (ancestor.equals(successor)) {
					return true;
				} else {
					Set<JobVertexID> newVirtualPredecessors = successorToPredecessors.get(ancestor.getID());
					if (newVirtualPredecessors != null) {
						for (JobVertexID newVirtualPredecessor : newVirtualPredecessors) {
							newAddedAncestors.add(executionGraph.getJobVertex(newVirtualPredecessor).getJobVertex());
						}
					}
					for (JobEdge jobEdge : ancestor.getInputs()) {
						newAddedAncestors.add(jobEdge.getSource().getProducer());
					}
				}
			}
			ancestors = newAddedAncestors;
		}
		return false;
	}

	/**
	 * Judge whether the job edge is the later read edge of a vertex.
	 * @return
	 */
	private boolean isStartOnFinishedEdge(JobEdge jobEdge) {
		JobVertex source = jobEdge.getSource().getProducer();
		for (JobControlEdge controlEdge : source.getInControlEdges()) {
			JobVertex controlEdgeSource = controlEdge.getSource();
			for (IntermediateDataSet output : controlEdgeSource.getProducedDataSets()) {
				for (JobEdge outputEdge : output.getConsumers()) {
					if (outputEdge.getTarget().equals(jobEdge.getTarget())) {
						return true;
					}
				}
			}
		}
		return false;
	}
}
