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

package com.alibaba.blink.launcher.autoconfig;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.StringUtils;

import com.alibaba.blink.launcher.autoconfig.rulebased.MiniBatchUtil;
import com.alibaba.blink.launcher.autoconfig.rulebased.StreamNodeUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * StreamNode properties.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(value = { "state_size" })
public class StreamNodeProperty extends AbstractJsonSerializable implements Comparable<StreamNodeProperty> {
	private int id;
	private String uid;
	private String name = "";
	private String pact = "";
	private String slotSharingGroup = "default";
	private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;

	@JsonProperty("parallelism")
	private int parallelism;

	@JsonProperty("maxParallelism")
	private int maxParallelism;

	@JsonProperty("vcore")
	private double cpuCores;

	@JsonProperty("heap_memory")
	private int heapMemoryInMB;

	@JsonProperty("direct_memory")
	private int directMemoryInMB;

	@JsonProperty("native_memory")
	private int nativeMemoryInMB;

	@JsonProperty("managed_memory")
	private int managedMemoryInMB;

	@JsonProperty("lock")
	private List<String> lock;

	@JsonProperty("gpu")
	private double gpuLoad;

	@JsonProperty("work_dir_vssd")
	private int workDirVssdLoad;

	@JsonProperty("log_dir_vssd")
	private int logDirVssdLoad;

	@JsonProperty("state_native_memory")
	private int stateNativeMemoryInMB;

	@JsonProperty("otherResources")
	private Map<String, Double> otherResources;

	@JsonProperty("resourceConstraints")
	private Map<String, String> resourceConstraints;

	@JsonProperty("minibatch_allow_latency")
	private long miniBatchAllowLatency;

	@JsonProperty("minibatch_size")
	private long miniBatchSize;

	public StreamNodeProperty() {
	}

	public StreamNodeProperty(int id) {
		this.id = id;
	}

	public void update(StreamNodeProperty streamNodeProperties) {
		this.uid = streamNodeProperties.getUid();
		this.parallelism = streamNodeProperties.getParallelism();
		this.maxParallelism = streamNodeProperties.getMaxParallelism();
		this.slotSharingGroup = streamNodeProperties.getSlotSharingGroup();
		this.chainingStrategy = streamNodeProperties.getChainingStrategy();
		this.cpuCores = streamNodeProperties.getCpuCores();
		this.heapMemoryInMB = streamNodeProperties.getHeapMemoryInMB();
		this.directMemoryInMB = streamNodeProperties.getDirectMemoryInMB();
		this.nativeMemoryInMB = streamNodeProperties.getNativeMemoryInMB();
		this.managedMemoryInMB = streamNodeProperties.getManagedMemoryInMB();

		if (streamNodeProperties.getLock() != null) {
			this.lock = new ArrayList<>(streamNodeProperties.getLock());
		}

		this.gpuLoad = streamNodeProperties.getGpuLoad();
		this.workDirVssdLoad = streamNodeProperties.getWorkDirVssdLoad();
		this.logDirVssdLoad = streamNodeProperties.getLogDirVssdLoad();
		this.stateNativeMemoryInMB = streamNodeProperties.getStateNativeMemoryInMB();
		this.miniBatchAllowLatency = streamNodeProperties.getMiniBatchAllowLatency();
		this.miniBatchSize = streamNodeProperties.getMiniBatchSize();
		if (streamNodeProperties.otherResources != null) {
			this.otherResources = new HashMap<>(streamNodeProperties.otherResources);
		}
		if (streamNodeProperties.resourceConstraints != null) {
			this.resourceConstraints = new LinkedHashMap<>(streamNodeProperties.resourceConstraints);
		}
	}

	public int getId() {
		return id;
	}

	public String getUid() {
		return uid == null ? String.valueOf(id) : uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSlotSharingGroup() {
		return slotSharingGroup;
	}

	public void setSlotSharingGroup(String slotSharingGroup) {
		this.slotSharingGroup = slotSharingGroup;
	}

	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	public void setChainingStrategy(ChainingStrategy chainingStrategy) {
		this.chainingStrategy = chainingStrategy;
	}

	@JsonSetter("chainingStrategy")
	public void setChainingStrategy(String chainingStrategy) {
		final boolean isLegacyEmptyValue = chainingStrategy == null || chainingStrategy.equals("null")
			|| StringUtils.isNullOrWhitespaceOnly(chainingStrategy);
		if (!isLegacyEmptyValue) {
			this.chainingStrategy = ChainingStrategy.valueOf(chainingStrategy);
		}
	}

	public int getParallelism() {
		return parallelism;
	}

	public StreamNodeProperty setParallelism(int parallelism) {
		this.parallelism = parallelism;
		return this;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public void setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	public double getCpuCores() {
		return cpuCores;
	}

	public void setCpuCores(double cpuCores) {
		this.cpuCores = cpuCores;
	}

	public int getHeapMemoryInMB() {
		return heapMemoryInMB;
	}

	public void setHeapMemoryInMB(int heapMemoryInMB) {
		this.heapMemoryInMB = heapMemoryInMB;
	}

	public int getDirectMemoryInMB() {
		return directMemoryInMB;
	}

	public void setDirectMemoryInMB(int directMemoryInMB) {
		this.directMemoryInMB = directMemoryInMB;
	}

	public int getNativeMemoryInMB() {
		return nativeMemoryInMB;
	}

	public void setNativeMemoryInMB(int nativeMemoryInMB) {
		this.nativeMemoryInMB = nativeMemoryInMB;
	}

	public List<String> getLock() {
		return lock;
	}

	public void setLock(List<String> lock) {
		if (lock != null) {
			this.lock = new ArrayList(lock);
		}
	}

	public double getGpuLoad() {
		return gpuLoad;
	}

	public void setGpuLoad(double gpuLoad) {
		this.gpuLoad = gpuLoad;
	}

	public int getWorkDirVssdLoad() {
		return workDirVssdLoad;
	}

	public void setWorkDirVssdLoad(int workDirVssdLoad) {
		this.workDirVssdLoad = workDirVssdLoad;
	}

	public int getLogDirVssdLoad() {
		return logDirVssdLoad;
	}

	public void setLogDirVssdLoad(int logDirVssdLoad) {
		this.logDirVssdLoad = logDirVssdLoad;
	}

	public int getStateNativeMemoryInMB() {
		return stateNativeMemoryInMB;
	}

	public void setStateNativeMemoryInMB(int stateNativeMemoryInMB) {
		this.stateNativeMemoryInMB = stateNativeMemoryInMB;
	}

	public void setResource(String name, double value) {
		if (otherResources == null) {
			otherResources = new HashMap<>(1);
		}
		otherResources.put(name, value);
	}

	public Map<String, String> getResourceConstraints() {
		return resourceConstraints;
	}

	public void addResourceConstraint(String key, String value) {
		if (resourceConstraints == null) {
			resourceConstraints = new LinkedHashMap<>();
		}
		resourceConstraints.put(key, value);
	}

	public long getMiniBatchAllowLatency() {
		return miniBatchAllowLatency;
	}

	public void setMiniBatchAllowLatency(long miniBatchAllowLatency) {
		this.miniBatchAllowLatency = miniBatchAllowLatency;
	}

	public long getMiniBatchSize() {
		return miniBatchSize;
	}

	public void setMiniBatchSize(long miniBatchSize) {
		this.miniBatchSize = miniBatchSize;
	}

	@Override
	public int compareTo(StreamNodeProperty streamNodeProperties) {
		return this.id - streamNodeProperties.getId();
	}

	public void apple(StreamNode node) {
		// CONSIDER: find a better way to identify transformation with StreamNode, so that we can better
		// detect mismatch between JSON and stream graph.
		if (node != null) {
			node.getOperator().setChainingStrategy(chainingStrategy);
			node.setParallelism(parallelism);
			StreamNodeUtil.setMaxParallelism(node, maxParallelism);

			ResourceSpec.Builder builder = ResourceSpec.newBuilder()
					.setCpuCores(cpuCores)
					.setHeapMemoryInMB(heapMemoryInMB)
					.setDirectMemoryInMB(directMemoryInMB)
					.setNativeMemoryInMB(nativeMemoryInMB);

			if (gpuLoad > 0) {
				builder.setGPUResource(gpuLoad);
			}

			if (otherResources != null) {
				for (Map.Entry<String, Double> entry : otherResources.entrySet()) {
					builder.addExtendedResource(new CommonExtendedResource(entry.getKey(), entry.getValue()));
				}
			}
			if (managedMemoryInMB > 0) {
				builder.addExtendedResource(new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
			}

			ResourceSpec resourceSpec = builder.build();
			node.setResources(resourceSpec, resourceSpec);
			MiniBatchUtil.tryApplyMiniBatchProperties(this, node);
		}
	}

	public int getManagedMemoryInMB() {
		return managedMemoryInMB;
	}

	public void setManagedMemoryInMB(int managedMemoryInMB) {
		this.managedMemoryInMB = managedMemoryInMB;
	}

	public String getPact() {
		return pact;
	}

	public void setPact(String pact) {
		this.pact = pact;
	}
}
