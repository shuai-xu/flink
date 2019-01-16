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
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.JobGraphOverviewHandler;
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
 * Response type of the {@link JobGraphOverviewHandler}.
 */
public class JobGraphOverviewInfo implements ResponseBody {
	public static final String FIELD_NAME_JOB_CONFIG = "config";
	public static final String FIELD_NAME_VERTEX_CONFIG = "vertex-config";
	public static final String FIELD_NAME_INPUT_NODES = "input-nodes";

	@JsonProperty(FIELD_NAME_JOB_CONFIG)
	private final Configuration config;

	@JsonProperty(FIELD_NAME_VERTEX_CONFIG)
	private final Map<String, VertexConfigInfo> vertexConfigs;

	@JsonProperty(FIELD_NAME_INPUT_NODES)
	private final Map<String, List<String>> inputNodes;

	@JsonCreator
	public JobGraphOverviewInfo(
			@JsonProperty(FIELD_NAME_JOB_CONFIG) Configuration config,
			@JsonProperty(FIELD_NAME_VERTEX_CONFIG) Map<String, VertexConfigInfo> vertexConfigs,
			@JsonProperty(FIELD_NAME_INPUT_NODES) Map<String, List<String>> inputNodes) {
		this.config = config;
		this.vertexConfigs = vertexConfigs;
		this.inputNodes = inputNodes;
	}

	public Configuration getConfig() {
		return config;
	}

	public Map<String, VertexConfigInfo> getVertexConfigs() {
		return vertexConfigs;
	}

	public Map<String, List<String>> getInputNodes() {
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
		private final ResourceSpecInfo resourceSpec;

		@JsonProperty(FIELD_NAME_NODE_IDS)
		private final List<Integer> nodeIds;

		@JsonCreator
		public VertexConfigInfo(
			@JsonDeserialize(using = JobVertexIDDeserializer.class) @JsonProperty(FIELD_NAME_VERTEX_ID) JobVertexID id,
			@JsonProperty(FIELD_NAME_VERTEX_NAME) String name,
			@JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
			@JsonProperty(FIELD_NAME_MAX_PARALLELISM) int maxParallelism,
			@JsonProperty(FIELD_NAME_RESOURCE_SPEC) ResourceSpecInfo resourceSpec,
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
		public ResourceSpecInfo getResourceSpec() {
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

	/**
	 * ResourceSpec Info class.
	 */
	public static final class ResourceSpecInfo {
		public static final String FIELD_NAME_CPU_CORE = "cpu-cores";
		public static final String FIELD_NAME_HEAP_MEMORY = "heap-memory";
		public static final String FIELD_NAME_DIRECT_MEMORY = "direct-memory";
		public static final String FIELD_NAME_NATIVE_MEMORY = "native-memory";
		public static final String FIELD_NAME_STATE_SIZE = "state-size";
		public static final String FIELD_NAME_EXTENDED_RESOURCES = "extended-resources";

		@JsonProperty(FIELD_NAME_CPU_CORE)
		private final double cpuCores;

		@JsonProperty(FIELD_NAME_HEAP_MEMORY)
		private final int heapMemoryInMB;

		@JsonProperty(FIELD_NAME_DIRECT_MEMORY)
		private final int directMemoryInMB;

		@JsonProperty(FIELD_NAME_NATIVE_MEMORY)
		private final int nativeMemoryInMB;

		@JsonProperty(FIELD_NAME_STATE_SIZE)
		private final int stateSizeInMB;

		@JsonProperty(FIELD_NAME_EXTENDED_RESOURCES)
		private final Map<String, Resource> extendedResources;

		@JsonCreator
		public ResourceSpecInfo(
			@JsonProperty(FIELD_NAME_CPU_CORE) double cpuCores,
			@JsonProperty(FIELD_NAME_HEAP_MEMORY) int heapMemoryInMB,
			@JsonProperty(FIELD_NAME_DIRECT_MEMORY) int directMemoryInMB,
			@JsonProperty(FIELD_NAME_NATIVE_MEMORY) int nativeMemoryInMB,
			@JsonProperty(FIELD_NAME_STATE_SIZE) int stateSizeInMB,
			@JsonProperty(FIELD_NAME_EXTENDED_RESOURCES) Map<String, Resource> extendedResources) {
			this.cpuCores = cpuCores;
			this.heapMemoryInMB = heapMemoryInMB;
			this.nativeMemoryInMB = nativeMemoryInMB;
			this.directMemoryInMB = directMemoryInMB;
			this.stateSizeInMB = stateSizeInMB;
			this.extendedResources = extendedResources;
		}

		@JsonIgnore
		public double getCpuCores() {
			return cpuCores;
		}

		@JsonIgnore
		public int getHeapMemoryInMB() {
			return heapMemoryInMB;
		}

		@JsonIgnore
		public int getDirectMemoryInMB() {
			return directMemoryInMB;
		}

		@JsonIgnore
		public int getNativeMemoryInMB() {
			return nativeMemoryInMB;
		}

		@JsonIgnore
		public int getStateSizeInMB() {
			return stateSizeInMB;
		}

		@JsonIgnore
		public Map<String, Resource> getExtendedResources() {
			return extendedResources;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			ResourceSpecInfo that = (ResourceSpecInfo) o;
			return cpuCores == that.cpuCores &&
				heapMemoryInMB == that.heapMemoryInMB &&
				directMemoryInMB == that.directMemoryInMB &&
				nativeMemoryInMB == that.nativeMemoryInMB &&
				stateSizeInMB == that.stateSizeInMB &&
				Objects.equals(extendedResources, that.extendedResources);
		}

		@Override
		public int hashCode() {
			return Objects.hash(cpuCores, heapMemoryInMB, directMemoryInMB,
				nativeMemoryInMB, stateSizeInMB, extendedResources);
		}

		public ResourceSpec convertToResourceSpec() {
			Resource[] resources;
			if (extendedResources != null && extendedResources.size() > 0) {
				resources = extendedResources.values().toArray(new Resource[extendedResources.size()]);
			} else {
				resources = new Resource[0];
			}
			return ResourceSpec.newBuilder()
				.setCpuCores(this.cpuCores)
				.setHeapMemoryInMB(this.heapMemoryInMB)
				.setDirectMemoryInMB(this.directMemoryInMB)
				.setNativeMemoryInMB(this.directMemoryInMB).addExtendedResource(resources).build();
		}
	}
}
