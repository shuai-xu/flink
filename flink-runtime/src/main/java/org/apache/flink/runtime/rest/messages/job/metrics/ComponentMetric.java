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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Objects;

/**
 * Response type for component metrics.
 */
public class ComponentMetric {

	private static final String FIELD_NAME_COMPONENT_ID = "component-id";

	private static final String FIELD_NAME_TIMETAMP = "timestamp";

	private static final String FIELD_NAME_METRICS = "metrics";

	@JsonProperty(value = FIELD_NAME_COMPONENT_ID)
	private final String componentId;

	@JsonProperty(FIELD_NAME_TIMETAMP)
	private final long timestamp;

	@JsonProperty(FIELD_NAME_METRICS)
	private final Collection<Metric> metrics;

	@JsonCreator
	public ComponentMetric(
		final @JsonProperty(value = FIELD_NAME_COMPONENT_ID) String componentId,
		final @JsonProperty(FIELD_NAME_TIMETAMP) long  timestamp,
		final @JsonProperty(FIELD_NAME_METRICS) Collection<Metric> metrics) {
		this.componentId = componentId;
		this.timestamp = timestamp;
		this.metrics = metrics;
	}

	@JsonIgnore
	public String getComponentId() {
		return componentId;
	}

	@JsonIgnore
	public long getTimestamp() {
		return timestamp;
	}

	@JsonIgnore
	public Collection<Metric> getMetrics() {
		return metrics;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		ComponentMetric that = (ComponentMetric) o;
		return Objects.equals(componentId, that.componentId) &&
			timestamp == that.timestamp &&
			Objects.equals(metrics, that.metrics);
	}

	@Override
	public int hashCode() {
		return Objects.hash(componentId, timestamp, metrics);
	}
}
