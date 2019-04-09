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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ConcurrentSchedulingGroup contains the execution vertices which should be scheduled at the same time.
 */
public class ConcurrentSchedulingGroup {

	private final List<ExecutionVertex> executionVertices;

	private final boolean hasPrecedingGroup;

	private final AtomicBoolean scheduled;

	ConcurrentSchedulingGroup(
			List<ExecutionVertex> executionVertices,
			boolean hasPrecedingGroup) {
		this(executionVertices, hasPrecedingGroup, false);
	}

	ConcurrentSchedulingGroup(
			List<ExecutionVertex> executionVertices,
			boolean hasPrecedingGroup,
			boolean scheduled) {
		this.executionVertices = executionVertices;
		this.hasPrecedingGroup = hasPrecedingGroup;
		this.scheduled = new AtomicBoolean(scheduled);
	}

	public List<ExecutionVertex> getExecutionVertices() {
		return executionVertices;
	}

	public boolean hasPrecedingGroup() {
		return hasPrecedingGroup;
	}

	public boolean markScheduled() {
		return scheduled.compareAndSet(false, true);
	}
}
