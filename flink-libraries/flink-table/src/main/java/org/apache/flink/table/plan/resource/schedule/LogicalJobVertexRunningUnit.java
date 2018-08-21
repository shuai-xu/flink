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

import org.apache.flink.runtime.scheduler.LogicalTask;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * RunningUnit consist of logical jobVertex.
 */
public class LogicalJobVertexRunningUnit {

	private final Set<LogicalJobVertex> jobVertexSet = new LinkedHashSet<>();
	private final Set<RelStageID> dependRelStageSet = new HashSet<>();

	public void addDependRelStage(RelStageID relStageID) {
		dependRelStageSet.add(relStageID);
	}

	public LogicalJobVertexRunningUnit(Set<LogicalJobVertex> jobVertexSet) {
		this.jobVertexSet.addAll(jobVertexSet);
	}

	// exclude deployed tasks.
	public List<LogicalTask> getToScheduleTasks() {
		List<LogicalTask> tasks = new LinkedList<>();
		for (LogicalJobVertex jobVertex : jobVertexSet) {
			if (!jobVertex.allTasksDeploying()) {
				tasks.addAll(jobVertex.getTasks());
			}
		}
		return tasks;
	}

	// check all jobs whether all tasks deployed.
	public boolean allTasksDeploying() {
		for (LogicalJobVertex job : jobVertexSet) {
			if (!job.allTasksDeploying()) {
				return false;
			}
		}
		return true;
	}

	public void receiveEventID(RelStageID relStageID) {
		dependRelStageSet.remove(relStageID);
	}

	public boolean dependEventsReady() {
		return dependRelStageSet.isEmpty();
	}

	public Set<LogicalJobVertex> getJobVertexSet() {
		return jobVertexSet;
	}
}
