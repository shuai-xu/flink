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
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ResourceSpec Info class.
 */
public class ResourceSpecInfo implements ResponseBody, Serializable {

	public static final String FIELD_NAME_CPU_CORE = "cpu-cores";
	public static final String FIELD_NAME_HEAP_MEMORY = "heap-memory";
	public static final String FIELD_NAME_DIRECT_MEMORY = "direct-memory";
	public static final String FIELD_NAME_NATIVE_MEMORY = "native-memory";
	public static final String FIELD_NAME_STATE_SIZE = "state-size";
	public static final String FIELD_NAME_EXTENDED_RESOURCES = "extended-resources";
	private static final long serialVersionUID = -6021783093094354286L;

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
	private final Map<String, ResourceInfo> extendedResources;

	@JsonCreator
	public ResourceSpecInfo(
		@JsonProperty(FIELD_NAME_CPU_CORE) double cpuCores,
		@JsonProperty(FIELD_NAME_HEAP_MEMORY) int heapMemoryInMB,
		@JsonProperty(FIELD_NAME_DIRECT_MEMORY) int directMemoryInMB,
		@JsonProperty(FIELD_NAME_NATIVE_MEMORY) int nativeMemoryInMB,
		@JsonProperty(FIELD_NAME_STATE_SIZE) int stateSizeInMB,
		@JsonProperty(FIELD_NAME_EXTENDED_RESOURCES) Map<String, ResourceInfo> extendedResources) {
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
	public Map<String, ResourceInfo> getExtendedResources() {
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
			List<Resource> resourcesTmp = extendedResources.values().stream().map(ResourceInfo::convertToResource).collect(Collectors.toList());
			resources = resourcesTmp.toArray(new Resource[extendedResources.size()]);
		} else {
			resources = new Resource[0];
		}
		return ResourceSpec.newBuilder()
			.setCpuCores(this.cpuCores)
			.setHeapMemoryInMB(this.heapMemoryInMB)
			.setDirectMemoryInMB(this.directMemoryInMB)
			.setNativeMemoryInMB(this.nativeMemoryInMB)
			.setStateSizeInMB(this.stateSizeInMB)
			.addExtendedResource(resources).build();
	}

	/**
	 * json format for org.apache.flink.api.common.resources.Resource.
	 */
	public static final class ResourceInfo {

		public static final String FIELD_NAME_RESOURCE_NAME = "name";
		public static final String FIELD_NAME_RESOURCE_VALUE = "value";
		public static final String FIELD_NAME_RESOURCE_AGGREGATE_TYPE = "resource-aggregate-type";

		@JsonProperty(FIELD_NAME_RESOURCE_NAME)
		private final String name;

		@JsonProperty(FIELD_NAME_RESOURCE_VALUE)
		private final Double value;

		@JsonProperty(FIELD_NAME_RESOURCE_AGGREGATE_TYPE)
		private final Resource.ResourceAggregateType  resourceAggregateType;

		@JsonCreator
		public ResourceInfo (
			@JsonProperty(FIELD_NAME_RESOURCE_NAME) String name,
			@JsonProperty(FIELD_NAME_RESOURCE_VALUE) Double value,
			@JsonProperty(FIELD_NAME_RESOURCE_AGGREGATE_TYPE) Resource.ResourceAggregateType resourceAggregateType) {
			this.name = name;
			this.value = value;
			this.resourceAggregateType = resourceAggregateType;
		}

		public ResourceInfo (Resource resource) {
			this.name = resource.getName();
			this.value = resource.getValue();
			this.resourceAggregateType = resource.getResourceAggregateType();
		}

		@JsonIgnore
		public String getName() {
			return name;
		}

		@JsonIgnore
		public Double getValue() {
			return value;
		}

		@JsonIgnore
		public Resource.ResourceAggregateType  getResourceAggregateType() {
			return resourceAggregateType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			ResourceInfo that = (ResourceInfo) o;
			return Objects.equals(name, that.name) &&
				value == that.value &&
				Objects.equals(resourceAggregateType, that.resourceAggregateType);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, value, resourceAggregateType);
		}

		public Resource convertToResource() {
			return new CommonExtendedResource(this.name, this.value, this.resourceAggregateType);
		}
	}
}
