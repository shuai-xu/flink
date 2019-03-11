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

import org.apache.flink.runtime.rest.handler.job.rescaling.JobUpdateJobConfigHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response type of the {@link JobUpdateJobConfigHandler}.
 */
public class JobUpdatingConfigStatus implements ResponseBody {

	public static final String FIELD_NAME_UPDATING_CONFIG_STATUS = "success";

	@JsonProperty(FIELD_NAME_UPDATING_CONFIG_STATUS)
	private final boolean success;

	public JobUpdatingConfigStatus(
		@JsonProperty(FIELD_NAME_UPDATING_CONFIG_STATUS) boolean success) {
		this.success = success;
	}

	@JsonIgnore
	public boolean isSuccess() {
		return success;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobUpdatingConfigStatus that = (JobUpdatingConfigStatus) o;
		return that.success == this.success;
	}

	@Override
	public int hashCode() {
		return Objects.hash(success);
	}

}
