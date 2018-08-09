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

import org.apache.flink.runtime.jobgraph.ExecutionVertexID;

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
	 * Get the status of the execution vertex. Including execution state, whether input data is ready to consume, etc.
	 *
	 * @param executionVertexID id of the vertex to query
	 * @return status of the execution vertex
	 */
	ExecutionVertexStatus getExecutionVertexStatus(ExecutionVertexID executionVertexID);
}
