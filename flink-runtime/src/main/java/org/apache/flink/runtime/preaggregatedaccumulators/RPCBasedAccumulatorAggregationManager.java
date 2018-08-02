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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

/**
 * A specialized implementation based on the FLIP-6 architecture.
 */
public class RPCBasedAccumulatorAggregationManager implements AccumulatorAggregationManager {

	@Override
	public void registerPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, ExecutionAttemptID attemptId, String name) {

	}

	@Override
	public void commitPreAggregatedAccumulator(JobID jobId, JobVertexID jobVertexId, ExecutionAttemptID attemptId, String name, Accumulator value) {

	}

	@Override
	public <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(JobID jobId, ExecutionAttemptID attemptId, String name) {
		return new CompletableFuture<>();
	}
}
