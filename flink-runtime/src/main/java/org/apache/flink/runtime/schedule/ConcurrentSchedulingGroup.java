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

import java.util.HashSet;
import java.util.Set;

/**
 * ConcurrentSchedulingGroup contains the execution vertices which should be scheduled at the same time.
 */
public class ConcurrentSchedulingGroup {

	private final Set<ExecutionVertexID> executionVertices;

	private final boolean hasPrecedingGroup;

	ConcurrentSchedulingGroup(Set<ExecutionVertexID> executionVertexIDs, boolean hasPrecedingGroup) {
		this.hasPrecedingGroup = hasPrecedingGroup;
		this.executionVertices = new HashSet<>(executionVertexIDs.size());
		executionVertices.addAll(executionVertexIDs);
	}

	public Set<ExecutionVertexID> getExecutionVertices() {
		return executionVertices;
	}

	public boolean hasPrecedingGroup() {
		return hasPrecedingGroup;
	}

	Set<ExecutionVertexID> merge(ConcurrentSchedulingGroup another) {
		Set<ExecutionVertexID> missingExecutionVertices = new HashSet<>();

		for (ExecutionVertexID executionVertexID : another.getExecutionVertices()) {
			if (!executionVertices.contains(executionVertexID)) {
				missingExecutionVertices.add(executionVertexID);
			}
		}

		executionVertices.addAll(missingExecutionVertices);
		return missingExecutionVertices;
	}
}
