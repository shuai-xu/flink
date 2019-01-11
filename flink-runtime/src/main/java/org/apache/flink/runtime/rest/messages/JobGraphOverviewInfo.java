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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Response type of the {@link JobVertexDetailsHandler}.
 */
public class JobGraphOverviewInfo implements ResponseBody {
	public static final String FIELD_NAME_JOB_CONFIG = "config";
	public static final String FIELD_NAME_VERTEX_CONFIG = "vertex-config";
	public static final String FIELD_NAME_INPUT_NODES = "input-nodes";

	@JsonProperty(FIELD_NAME_JOB_CONFIG)
	private final Configuration config;

	@JsonProperty(FIELD_NAME_VERTEX_CONFIG)
	private final Map<JobVertexID, VertexConfigInfo> vertexConfigs;

	@JsonProperty(FIELD_NAME_INPUT_NODES)
	private final Map<JobVertexID, List<JobVertexID>> inputNodes;

	@JsonCreator
	public JobGraphOverviewInfo(
			@JsonProperty(FIELD_NAME_JOB_CONFIG) Configuration config,
			@JsonProperty(FIELD_NAME_VERTEX_CONFIG) Map<JobVertexID, VertexConfigInfo> vertexConfigs,
			@JsonProperty(FIELD_NAME_INPUT_NODES) Map<JobVertexID, List<JobVertexID>> inputNodes) {
		this.config = config;
		this.vertexConfigs = vertexConfigs;
		this.inputNodes = inputNodes;
	}

	@JsonIgnore
	public Configuration getConfig() {
		return config;
	}

	@JsonIgnore
	public Map<JobVertexID, VertexConfigInfo> getVertexConfigs() {
		return vertexConfigs;
	}

	@JsonIgnore
	public Map<JobVertexID, List<JobVertexID>> getInputNodes() {
		return inputNodes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		JobGraphOverviewInfo that = (JobGraphOverviewInfo) o;
		return Objects.equals(config, that.config) &&
			Objects.equals(vertexConfigs, that.vertexConfigs) &&
			Objects.equals(inputNodes, that.inputNodes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(config, vertexConfigs, inputNodes);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Vertex config class.
	 */
	public static final class VertexConfigInfo {
		public static final String FIELD_NAME_VERTEX_ID = "id";
		public static final String FIELD_NAME_VERTEX_NAME = "name";
		public static final String FIELD_NAME_PARALLELISM = "parallelism";
		public static final String FIELD_NAME_MAX_PARALLELISM = "max-parallelism";
		public static final String FIELD_NAME_RESOURCE_SPEC = "resource-spec";
		public static final String FIELD_NAME_NODE_IDS = "nodeIds";

		@JsonProperty(FIELD_NAME_VERTEX_ID)
		@JsonSerialize(using = JobVertexIDSerializer.class)
		private final JobVertexID id;

		@JsonProperty(FIELD_NAME_VERTEX_NAME)
		private final String name;

		@JsonProperty(FIELD_NAME_PARALLELISM)
		private final int parallelism;

		@JsonProperty(FIELD_NAME_MAX_PARALLELISM)
		private final int maxParallelism;

		@JsonProperty(FIELD_NAME_RESOURCE_SPEC)
		private final ResourceSpec resourceSpec;

		@JsonProperty(FIELD_NAME_NODE_IDS)
		private final List<Integer> nodeIds;

		@JsonCreator
		public VertexConfigInfo(
			@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID id,
			@JsonProperty(FIELD_NAME_VERTEX_NAME) String name,
			@JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
			@JsonProperty(FIELD_NAME_MAX_PARALLELISM) int maxParallelism,
			@JsonProperty(FIELD_NAME_RESOURCE_SPEC) ResourceSpec resourceSpec,
			@JsonProperty(FIELD_NAME_NODE_IDS) List<Integer> nodeIds) {
			this.id = checkNotNull(id);
			this.name = checkNotNull(name);
			this.parallelism = parallelism;
			this.maxParallelism = maxParallelism;
			this.resourceSpec = resourceSpec;
			this.nodeIds = nodeIds;
		}

		@JsonIgnore
		public JobVertexID getId() {
			return id;
		}

		@JsonIgnore
		public String getName() {
			return name;
		}

		@JsonIgnore
		public int getParallelism() {
			return parallelism;
		}

		@JsonIgnore
		public int getMaxParallelism() {
			return maxParallelism;
		}

		@JsonIgnore
		public ResourceSpec getResourceSpec() {
			return resourceSpec;
		}

		@JsonIgnore
		public List<Integer> getNodeIds() {
			return nodeIds;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			VertexConfigInfo that = (VertexConfigInfo) o;
			return Objects.equals(id, that.id) &&
				Objects.equals(name, that.name) &&
				parallelism == that.parallelism &&
				Objects.equals(resourceSpec, that.resourceSpec) &&
				Objects.equals(nodeIds, that.nodeIds);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, parallelism, resourceSpec, nodeIds);
		}
	}
}
