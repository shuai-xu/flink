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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskexecutor.JobManagerConnection;
import org.apache.flink.runtime.taskexecutor.JobManagerTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A specialized implementation based on the FLIP-6 architecture.
 */
public class RPCBasedAccumulatorAggregationManager implements AccumulatorAggregationManager {
	private final JobManagerTable jobManagerTable;

	private final Map<JobID, Map<String, AggregatedAccumulator>> perJobAccumulators = new HashMap<>();

	public RPCBasedAccumulatorAggregationManager(JobManagerTable jobManagerTable) {
		this.jobManagerTable = jobManagerTable;
	}

	@Override
	public void registerPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, ExecutionAttemptID attemptId, String name) {
		synchronized (perJobAccumulators) {
			AggregatedAccumulator aggregatedAccumulator = perJobAccumulators.computeIfAbsent(jobId, k -> new HashMap<>())
				.computeIfAbsent(name, k -> new AggregatedAccumulator(jobVertexId));
			aggregatedAccumulator.registerForTask(jobVertexId, attemptId);
		}
	}

	@Override
	public void commitPreAggregatedAccumulator(JobID jobId, ExecutionAttemptID attemptId, String name, Accumulator value) {
		synchronized (perJobAccumulators) {
			Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAccumulators.get(jobId);
			AggregatedAccumulator aggregatedAccumulator = (currentJobAccumulators != null ? currentJobAccumulators.get(name) : null);

			checkState(aggregatedAccumulator != null, "The committed accumulator does not exist.");

			aggregatedAccumulator.commitForTask(attemptId, value);

			if (aggregatedAccumulator.isAllCommitted()) {
				commitAggregatedAccumulators(jobId,
					Collections.singletonList(new CommitAccumulator(aggregatedAccumulator.getJobVertexId(),
						name,
						aggregatedAccumulator.getAggregatedValue(),
						aggregatedAccumulator.getCommittedTasks())));

				// Remove the accumulator no matter whether its value is reported to JobMaster.
				currentJobAccumulators.remove(name);
			}

			if (currentJobAccumulators.isEmpty()) {
				perJobAccumulators.remove(jobId);
			}
		}
	}

	@Override
	public <V, A extends
		Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(JobID jobId,
																						ExecutionAttemptID attemptId, String name) {
		return new CompletableFuture<>();
	}

	@Override
	public void clearRegistrationForTask(JobID jobId, ExecutionAttemptID attemptId) {
		synchronized (perJobAccumulators) {
			Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAccumulators.get(jobId);

			if (currentJobAccumulators != null) {
				List<CommitAccumulator> commitAccumulators = new ArrayList<>();
				List<String> shouldRemove = new ArrayList<>();

				for (Map.Entry<String, AggregatedAccumulator> entry : currentJobAccumulators.entrySet()) {
					AggregatedAccumulator aggregatedAccumulator = entry.getValue();

					aggregatedAccumulator.clearRegistrationForTask(attemptId);

					if (aggregatedAccumulator.isAllCommitted()) {
						commitAccumulators.add(new CommitAccumulator(entry.getValue().getJobVertexId(),
							entry.getKey(),
							entry.getValue().getAggregatedValue(),
							entry.getValue().getCommittedTasks()));
					}

					if (aggregatedAccumulator.isAllCommitted() || aggregatedAccumulator.isEmpty()) {
						shouldRemove.add(entry.getKey());
					}
				}

				if (commitAccumulators.size() > 0) {
					commitAggregatedAccumulators(jobId, commitAccumulators);
				}

				for (String name : shouldRemove) {
					currentJobAccumulators.remove(name);
				}

				if (currentJobAccumulators.isEmpty()) {
					perJobAccumulators.remove(jobId);
				}
			}
		}
	}

	@Override
	public void clearAccumulatorsForJob(JobID jobId) {
		synchronized (perJobAccumulators) {
			Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAccumulators.remove(jobId);

			if (currentJobAccumulators != null) {
				currentJobAccumulators.clear();
			}
		}
	}

	@VisibleForTesting
	Map<JobID, Map<String, AggregatedAccumulator>> getPerJobAccumulators() {
		return perJobAccumulators;
	}

	private void commitAggregatedAccumulators(JobID jobId, List<CommitAccumulator> accumulators) {
		assert Thread.holdsLock(perJobAccumulators);

		JobManagerConnection connection = jobManagerTable.get(jobId);
		if (connection != null) {
			connection.getJobManagerGateway().commitPreAggregatedAccumulator(accumulators);
		}
	}

	/**
	 * The wrapper class for an accumulator, which manages its registered tasks and committed tasks.
	 */
	static final class AggregatedAccumulator {
		private final Set<ExecutionAttemptID> registeredTasks = new HashSet<>();
		private final Set<ExecutionAttemptID> committedTasks = new HashSet<>();
		private final JobVertexID jobVertexId;

		private Accumulator aggregatedValue;

		AggregatedAccumulator(JobVertexID jobVertexId) {
			this.jobVertexId = jobVertexId;
		}

		void registerForTask(JobVertexID jobVertexId, ExecutionAttemptID attemptId) {
			checkArgument(this.jobVertexId.equals(jobVertexId),
				"The registered task belongs to different JobVertex with previous registered ones");

			checkState(!registeredTasks.contains(attemptId), "This task has already registered.");

			registeredTasks.add(attemptId);
		}

		@SuppressWarnings("unchecked")
		void commitForTask(ExecutionAttemptID attemptId, Accumulator value) {
			checkState(registeredTasks.contains(attemptId), "Can not commit for an accumulator that has " +
				"not been registered before");

			if (aggregatedValue == null) {
				aggregatedValue = value.clone();
			} else {
				aggregatedValue.merge(value);
			}

			committedTasks.add(attemptId);
		}

		void clearRegistrationForTask(ExecutionAttemptID attemptID) {
			if (registeredTasks.contains(attemptID) && !committedTasks.contains(attemptID)) {
				registeredTasks.remove(attemptID);
			}
		}

		boolean isEmpty() {
			return registeredTasks.size() == 0;
		}

		boolean isAllCommitted() {
			return registeredTasks.size() > 0 && registeredTasks.size() == committedTasks.size();
		}

		Accumulator getAggregatedValue() {
			return aggregatedValue;
		}

		JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		Set<ExecutionAttemptID> getCommittedTasks() {
			return committedTasks;
		}

		@VisibleForTesting
		Set<ExecutionAttemptID> getRegisteredTasks() {
			return registeredTasks;
		}
	}
}
