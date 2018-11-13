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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.ResourceIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ResourceIDSerializer;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMetricsInfo;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * overview about a job.
 */
public class JobOverview implements ResponseBody {

	public static final String FIELD_NAME_JOB_FAILOVER = "failover";

	public static final String FIELD_NAME_JOB_FAILOVER_HISTORY = "failover_history";

	public static final String FIELD_NAME_JOB_IN_QUEUE_FULL_VERTICES = "inqueue_full_vertices";

	public static final String FIELD_NAME_JOB_OUT_QUEUE_FULL_VERTICES = "outqueue_full_vertices";

	public static final String FIELD_NAME_JOB_TASKMANAGER = "taskmanagers";

	@JsonProperty(FIELD_NAME_JOB_FAILOVER)
	private final boolean failover;

	@JsonProperty(FIELD_NAME_JOB_FAILOVER_HISTORY)
	private final boolean failoverHistory;

	@JsonProperty(FIELD_NAME_JOB_IN_QUEUE_FULL_VERTICES)
	private final Collection<JobVertex> jobInQueueFullVertices;

	@JsonProperty(FIELD_NAME_JOB_OUT_QUEUE_FULL_VERTICES)
	private final Collection<JobVertex> jobOutQueueFullVertices;

	@JsonProperty(FIELD_NAME_JOB_TASKMANAGER)
	private final Collection<JobTaskManager> jobTaskManagers;

	@JsonCreator
	public JobOverview(
			@JsonProperty(FIELD_NAME_JOB_FAILOVER) boolean failover,
			@JsonProperty(FIELD_NAME_JOB_FAILOVER_HISTORY) boolean failoverHistory,
			@JsonProperty(FIELD_NAME_JOB_IN_QUEUE_FULL_VERTICES) Collection<JobVertex> jobInQueueFullVertices,
			@JsonProperty(FIELD_NAME_JOB_OUT_QUEUE_FULL_VERTICES) Collection<JobVertex> jobOutQueueFullVertices,
			@JsonProperty(FIELD_NAME_JOB_TASKMANAGER) Collection<JobTaskManager> jobTaskManagers) {
		this.failover = failover;
		this.failoverHistory = failoverHistory;
		this.jobInQueueFullVertices = jobInQueueFullVertices;
		this.jobOutQueueFullVertices = jobOutQueueFullVertices;
		this.jobTaskManagers = jobTaskManagers;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobOverview that = (JobOverview) o;
		return failover == that.failover &&
			failoverHistory == failoverHistory &&
			Objects.equals(jobInQueueFullVertices, that.jobInQueueFullVertices) &&
			Objects.equals(jobOutQueueFullVertices, that.jobOutQueueFullVertices) &&
			Objects.equals(jobTaskManagers, that.jobTaskManagers);
	}

	@Override
	public int hashCode() {
		return Objects.hash(failover, failoverHistory, jobInQueueFullVertices, jobOutQueueFullVertices, jobTaskManagers);
	}

	@JsonIgnore
	public boolean isFailover() {
		return failover;
	}

	@JsonIgnore
	public boolean isFailoverHistory() {
		return failoverHistory;
	}

	@JsonIgnore
	public Collection<JobVertex> getJobInQueueFullVertices() {
		return jobInQueueFullVertices;
	}

	@JsonIgnore
	public Collection<JobVertex> getJobOutQueueFullVertices() {
		return jobOutQueueFullVertices;
	}

	@JsonIgnore
	public Collection<JobTaskManager> getJobTaskManagers() {
		return jobTaskManagers;
	}

	// ---------------------------------------------------
	// Static inner classes
	// ---------------------------------------------------

	/**
	 * Detailed information about a job vertex subtask.
	 */
	public static final class JobSubtask {

		public static final String FIELD_NAME_VERTEX_ID = "vertex_id";

		public static final String FIELD_NAME_VERTEX_SUBTASK_INDEX = "subtask_index";

		public static final String FIELD_NAME_JOB_VERTEX_TASKMANAGER_ID = "taskmanager_id";

		public static final String FIELD_NAME_VERTEX_SUBTASK_METRICS = "metrics";

		@JsonProperty(FIELD_NAME_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID jobVertexID;

		@JsonProperty(FIELD_NAME_VERTEX_SUBTASK_INDEX)
		private final int index;

		@JsonProperty(FIELD_NAME_JOB_VERTEX_TASKMANAGER_ID)
		@JsonSerialize(using = ResourceIDSerializer.class)
		private final ResourceID taskmanagerId;

		@JsonProperty(FIELD_NAME_VERTEX_SUBTASK_METRICS)
		private final Map<String, String> metrics;

		@JsonCreator
		public JobSubtask(
				@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID jobVertexID,
				@JsonProperty(FIELD_NAME_VERTEX_SUBTASK_INDEX) int index,
				@JsonDeserialize(using = ResourceIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_VERTEX_TASKMANAGER_ID) ResourceID taskmanagerId,
				@JsonProperty(FIELD_NAME_VERTEX_SUBTASK_METRICS) Map<String, String> metrics) {
			this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
			this.index = index;
			this.taskmanagerId = Preconditions.checkNotNull(taskmanagerId);
			this.metrics = metrics;
		}

		@JsonIgnore
		public JobVertexID getJobVertexID() {
			return jobVertexID;
		}

		@JsonIgnore
		public int getIndex() {
			return index;
		}

		@JsonIgnore
		public ResourceID getTaskmanagerId() {
			return taskmanagerId;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobSubtask that = (JobSubtask) o;
			return Objects.equals(jobVertexID, that.jobVertexID) &&
				index == that.index &&
				Objects.equals(taskmanagerId, that.taskmanagerId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(jobVertexID, index, taskmanagerId);
		}
	}

	/**
	 * Overview about a job taskmanasger.
	 */
	public static final class JobTaskManager{

		public static final String FIELD_NAME_TASKMANAGER_ID = "id";

		public static final String FIELD_NAME_TASKMANAGER_VERTICES = "vertices";

		public static final String FIELD_NAME_TASKMANAGER_METRICS = "metrics";

		@JsonProperty(FIELD_NAME_TASKMANAGER_ID)
		@JsonSerialize(using = ResourceIDSerializer.class)
		private final ResourceID id;

		@JsonProperty(FIELD_NAME_TASKMANAGER_VERTICES)
		private final Collection<JobVertex> vertices;

		@JsonProperty(FIELD_NAME_TASKMANAGER_METRICS)
		private final TaskManagerMetricsInfo taskManagerMetrics;

		@JsonCreator
		public JobTaskManager(
			@JsonDeserialize(using = ResourceIDDeserializer.class)
			@JsonProperty(FIELD_NAME_TASKMANAGER_ID) ResourceID id,
			@JsonProperty(FIELD_NAME_TASKMANAGER_VERTICES) Collection<JobVertex> vertices,
			@JsonProperty(FIELD_NAME_TASKMANAGER_METRICS) TaskManagerMetricsInfo taskManagerMetrics) {
			this.id = Preconditions.checkNotNull(id);
			this.vertices = vertices;
			this.taskManagerMetrics = taskManagerMetrics;
		}

		@JsonIgnore
		public ResourceID getId() {
			return id;
		}

		@JsonIgnore
		public Collection<JobVertex> getVertices() {
			return vertices;
		}

		@JsonIgnore
		public TaskManagerMetricsInfo getTaskManagerMetrics() {
			return taskManagerMetrics;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobTaskManager that = (JobTaskManager) o;
			return Objects.equals(id, that.id) &&
					Objects.equals(vertices, that.vertices);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, vertices);
		}
	}

	/**
	 * overview about a job vertex.
	 */
	public static final class JobVertex {

		public static final String FIELD_NAME_VERTEX_ID = "id";

		public static final String FIELD_NAME_VERTEX_NAME = "name";

		public static final String FIELD_NAME_VERTEX_SUBTASKS = "subtasks";

		@JsonProperty(FIELD_NAME_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID id;

		@JsonProperty(FIELD_NAME_VERTEX_NAME)
		private final String name;

		@JsonProperty(FIELD_NAME_VERTEX_SUBTASKS)
		private final Collection<JobSubtask> subtasks;

		@JsonCreator
		public JobVertex(
			@JsonDeserialize(using = JobVertexIDDeserializer.class)
			@JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID id,
			@JsonProperty(FIELD_NAME_VERTEX_NAME) String name,
			@JsonProperty(FIELD_NAME_VERTEX_SUBTASKS) Collection<JobSubtask> subtasks) {
			this.id = Preconditions.checkNotNull(id);
			this.name = Preconditions.checkNotNull(name);
			this.subtasks = subtasks;
		}

		@JsonIgnore
		public JobVertexID getId() {
			return id;
		}

		@JsonIgnore
		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			JobVertex that = (JobVertex) o;
			return Objects.equals(id, that.id) &&
				Objects.equals(name, that.name) &&
				Objects.equals(subtasks, that.subtasks);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, subtasks);
		}
	}

}
