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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.util.Objects;

/**
 * ResourceSpec Info class.
 */
public class ExecutionVertexIDInfo implements ResponseBody, Serializable {

	public static final String FIELD_NAME_VERTEX_ID = "vertex-id";
	public static final String FIELD_NAME_TASK_INDEX = "task-index";

	@JsonProperty(FIELD_NAME_VERTEX_ID)
	private final JobVertexID jobVertexID;

	@JsonProperty(FIELD_NAME_TASK_INDEX)
	private final int taskIndex;

	@JsonCreator
	public ExecutionVertexIDInfo(
		@JsonDeserialize(using = JobVertexIDDeserializer.class)
		@JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID jobVertexID,
		@JsonProperty(FIELD_NAME_TASK_INDEX) int taskIndex) {
		this.jobVertexID = jobVertexID;
		this.taskIndex = taskIndex;
	}

	@JsonIgnore
	public JobVertexID getJobVertexID() {
		return jobVertexID;
	}

	@JsonIgnore
	public int getTaskIndex() {
		return taskIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		ExecutionVertexIDInfo that = (ExecutionVertexIDInfo) o;
		return Objects.equals(jobVertexID, that.jobVertexID) && taskIndex == that.taskIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobVertexID, taskIndex);
	}

	public ExecutionVertexID convertToResourceSpec() {
		return new ExecutionVertexID(jobVertexID, taskIndex);
	}
}
