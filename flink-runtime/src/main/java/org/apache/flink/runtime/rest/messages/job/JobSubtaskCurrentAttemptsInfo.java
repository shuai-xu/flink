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

import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * The sub task execution attempt response.
 */
public class JobSubtaskCurrentAttemptsInfo implements ResponseBody {

	public static final String FIELD_NAME_JOB_STATUS = "job_status";

	public static final String FIELD_NAME_SUBTASKS_INFO = "subtasks";

	@JsonProperty(FIELD_NAME_JOB_STATUS)
	private final JobStatus jobStatus;

	@JsonProperty(FIELD_NAME_SUBTASKS_INFO)
	private final Collection<SubtaskExecutionAttemptInfo> subtaskExecutionAttemptsInfo;

	@JsonCreator
	public JobSubtaskCurrentAttemptsInfo(
		@JsonProperty(FIELD_NAME_JOB_STATUS) JobStatus jobStatus,
		@JsonProperty(FIELD_NAME_SUBTASKS_INFO) Collection<SubtaskExecutionAttemptInfo> subtaskExecutionAttemptsInfo) {

		this.jobStatus = jobStatus;
		this.subtaskExecutionAttemptsInfo = subtaskExecutionAttemptsInfo;
	}

	public JobStatus getJobStatus() {
		return jobStatus;
	}

	@JsonIgnore
	public Collection<SubtaskExecutionAttemptInfo> getSubtaskInfos() {
		return subtaskExecutionAttemptsInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		JobSubtaskCurrentAttemptsInfo that = (JobSubtaskCurrentAttemptsInfo) o;

		return Objects.equals(jobStatus, that.jobStatus) &&
			Objects.equals(subtaskExecutionAttemptsInfo, that.subtaskExecutionAttemptsInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobStatus, subtaskExecutionAttemptsInfo);
	}

}
