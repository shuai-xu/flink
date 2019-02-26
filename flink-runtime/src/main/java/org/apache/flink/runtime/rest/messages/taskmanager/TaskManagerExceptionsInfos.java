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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerExceptionsHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.ResourceIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.ResourceIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Map;
import java.util.Objects;

/**
 * Response type of the {@link TaskManagerExceptionsHandler}.
 */
public class TaskManagerExceptionsInfos implements ResponseBody {

	private static final String FIELD_NAME_TASKMANAGER_EXCEPTIONS = "taskmanager-excetions";

	@JsonProperty(FIELD_NAME_TASKMANAGER_EXCEPTIONS)
	private final Map<Long, TaskManagerException> taskmanagerExceptions;

	@JsonCreator
	public TaskManagerExceptionsInfos(@JsonProperty(FIELD_NAME_TASKMANAGER_EXCEPTIONS) Map<Long, TaskManagerException> taskmanagerExceptions) {
		this.taskmanagerExceptions = taskmanagerExceptions;
	}

	@JsonIgnore
	public Map<Long, TaskManagerException> getTaskmanagerExceptions() {
		return taskmanagerExceptions;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TaskManagerExceptionsInfos that = (TaskManagerExceptionsInfos) o;
		return Objects.equals(taskmanagerExceptions, that.taskmanagerExceptions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(taskmanagerExceptions);
	}

	/**
	 * TaskManager Excetion.
	 */
	public static class TaskManagerException {

		private static final String FIELD_NAME_RESOURCE_ID = "resource-id";
		private static final String  FIELD_NAME_EXCEPTION = "exception";

		@JsonProperty(FIELD_NAME_RESOURCE_ID)
		@JsonSerialize(using = ResourceIDSerializer.class)
		private final ResourceID resourceId;

		@JsonProperty(FIELD_NAME_EXCEPTION)
		private final Exception exception;

		@JsonCreator
		public TaskManagerException(
			@JsonDeserialize(using = ResourceIDDeserializer.class) @JsonProperty(FIELD_NAME_RESOURCE_ID) ResourceID resourceId,
			@JsonProperty(FIELD_NAME_EXCEPTION) Exception e) {
			this.resourceId = resourceId;
			this.exception = e;
		}

		@JsonIgnore
		public ResourceID getResourceId() {
			return resourceId;
		}

		@JsonIgnore
		public Exception getException() {
			return exception;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			TaskManagerException that = (TaskManagerException) o;
			return Objects.equals(resourceId, that.resourceId) && Objects.equals(exception, that.exception);
		}

		@Override
		public int hashCode() {
			return Objects.hash(resourceId, exception);
		}
	}

}
