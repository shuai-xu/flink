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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collection;

/**
 * Basic interface to submit vertices and query their status.
 */
public interface VertexScheduler {

	// ------------------------------------------------------------------------
	//  Schedule Actions
	// ------------------------------------------------------------------------

	/**
	 * Submit execution vertices to scheduler.
	 *
	 * @param executionVertexIDs IDs of the vertices to schedule
	 */
	void scheduleExecutionVertices(Collection<ExecutionVertexID> executionVertexIDs);

	// ------------------------------------------------------------------------
	//  Vertex Status Queries
	// ------------------------------------------------------------------------

	/**
	 * Get the status of the execution vertex.
	 *
	 * @param executionVertexID id of the vertex to query
	 * @return status of the execution vertex
	 */
	ExecutionVertexStatus getExecutionVertexStatus(ExecutionVertexID executionVertexID);

	/**
	 * Get the status of the execution job vertex.
	 *
	 * @param jobVertexID id of the vertex to query
	 * @return status of the execution job vertex
	 */
	ExecutionState getExecutionJobVertexStatus(JobVertexID jobVertexID);

	/**
	 * Get the status of the result partition.
	 *
	 * @param resultID id of the result
	 * @param partitionNumber number of the partition in the result
	 * @return status of the result partition
	 */
	ResultPartitionStatus getResultPartitionStatus(IntermediateDataSetID resultID, int partitionNumber);

	/**
	 * Get the ratio of consumable partitions in the result.
	 *
	 * @param resultID id of the result
	 * @return ratio of consumable partitions in the result
	 */
	double getResultConsumablePartitionRatio(IntermediateDataSetID resultID);
}
