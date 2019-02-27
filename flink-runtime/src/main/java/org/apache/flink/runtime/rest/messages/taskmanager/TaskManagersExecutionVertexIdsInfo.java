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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * The list ExecutionVertexID response for all taskmanagers.
 */
public class TaskManagersExecutionVertexIdsInfo implements ResponseBody {

	public static final String FIELD_NAME_EXECUTION_VERTEX_ID_INFO = "execution-vertex-ids";

	@JsonProperty(FIELD_NAME_EXECUTION_VERTEX_ID_INFO)
	private final Map<String, TaskManagerExecutionVertexIdsInfo> executionVertexIds;

	@JsonCreator
	public TaskManagersExecutionVertexIdsInfo(
		@JsonProperty(FIELD_NAME_EXECUTION_VERTEX_ID_INFO) Map<String, TaskManagerExecutionVertexIdsInfo> executionVertexIds) {
		this.executionVertexIds = executionVertexIds;
	}

	@JsonIgnore
	public Map<String, TaskManagerExecutionVertexIdsInfo> getExecutionVertexIds() {
		return executionVertexIds;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskManagersExecutionVertexIdsInfo that = (TaskManagersExecutionVertexIdsInfo) o;

		return Objects.equals(executionVertexIds, that.executionVertexIds);
	}

	@Override
	public int hashCode() {
		return Objects.hash(executionVertexIds);
	}

}
