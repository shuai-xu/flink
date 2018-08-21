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

package org.apache.flink.table.plan.resource.schedule;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.LogicalTask;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Logical jobVertex.
 */
public class LogicalJobVertex {

	private final JobVertexID jobVertexID;
	private final Collection<? extends LogicalTask> tasks;
	private final AtomicInteger deployedNum = new AtomicInteger();

	public LogicalJobVertex(JobVertexID jobVertexID, Collection<? extends LogicalTask> tasks) {
		this.jobVertexID = jobVertexID;
		this.tasks = tasks;
	}

	public void taskScheduled() {
		deployedNum.incrementAndGet();
	}

	public boolean allTasksDeploying() {
		return deployedNum.get() >= tasks.size();
	}

	public Collection<? extends LogicalTask> getTasks() {
		return tasks;
	}

	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}
}
