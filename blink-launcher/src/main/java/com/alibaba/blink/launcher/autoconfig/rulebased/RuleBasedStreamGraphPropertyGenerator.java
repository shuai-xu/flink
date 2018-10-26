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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.table.api.InputPartitionSource;
import org.apache.flink.util.Preconditions;

import com.alibaba.blink.launcher.autoconfig.SourcePartitionFetcher;
import com.alibaba.blink.launcher.autoconfig.StreamEdgeProperty;
import com.alibaba.blink.launcher.autoconfig.StreamGraphProperty;
import com.alibaba.blink.launcher.autoconfig.StreamNodeProperty;
import com.alibaba.blink.launcher.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 *  RuleBasedStreamGraphPropertyGenerator base on rules.
 */
public class RuleBasedStreamGraphPropertyGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(RuleBasedStreamGraphPropertyGenerator.class);

	private Properties userProperties;
	private ResourceSettings resSettings;

	RuleBasedStreamGraphPropertyGenerator(Properties userProperties) {
		this.userProperties = userProperties;
		resSettings = new ResourceSettings(userProperties);
	}

	/**
	 * Generate json format of streaming graph.
	 *
	 * @param streamGraph streamGraph
	 * @return ResourceFile
	 */
	public StreamGraphProperty generateProperties(StreamGraph streamGraph) {
		Preconditions.checkNotNull(streamGraph, "streamGraph cannot be null");

		StreamGraphProperty properties = new StreamGraphProperty();

		Set<Integer> statefulNodes = new HashSet<>();

		// nodes
		for (StreamNode streamNode: streamGraph.getStreamNodes()) {
			int nodeId = streamNode.getId();
			StreamNodeProperty streamNodeProperties = new StreamNodeProperty(nodeId);
			if (streamNode.getOperator() != null && streamNode.getOperator().requireState()) {
				statefulNodes.add(nodeId);
			}

			streamNodeProperties.setUid(String.valueOf(nodeId));
			streamNodeProperties.setName(getNodeName(streamNode.getOperatorName()));
			if (streamNode.getInEdges().isEmpty()) {
				streamNodeProperties.setPact("Source");
			} else if (streamNode.getOutEdges().isEmpty()) {
				streamNodeProperties.setPact("Sink");
			} else {
				streamNodeProperties.setPact("Operator");
			}
			streamNodeProperties.setName(getNodeName(streamNode.getOperatorName()));
			streamNodeProperties.setSlotSharingGroup(streamNode.getSlotSharingGroup());
			streamNodeProperties.setChainingStrategy(streamNode.getOperator().getChainingStrategy());

			// set max parallelism according to origin max parallelism
			int nodeMaxParallel = StreamNodeUtil.getMaxParallelism(streamNode);
			int maxParallelism = nodeMaxParallel > 0 ? nodeMaxParallel : StreamGraphGenerator.UPPER_BOUND_MAX_PARALLELISM;
			streamNodeProperties.setMaxParallelism(maxParallelism);

			if (streamNode.getParallelism() > 0) {
				// parallelism configured already, not changed.
				streamNodeProperties.setParallelism(streamNode.getParallelism());
			} else {
				if (resSettings.getDefaultOperatorParallelism() > maxParallelism) {
					streamNodeProperties.setParallelism(maxParallelism);
					LOG.warn("Cannot set parallelism of streamNodeProperties id {} to {}.  Set to max parallelism {}.",
							nodeId, resSettings.getDefaultOperatorParallelism(), maxParallelism);
				} else {
					streamNodeProperties.setParallelism(resSettings.getDefaultOperatorParallelism());
				}
			}

			ResourceSpec minResourceSpec = streamNode.getMinResources();
			if (minResourceSpec != null && !minResourceSpec.equals(ResourceSpec.DEFAULT)) {
				streamNodeProperties.setCpuCores(minResourceSpec.getCpuCores());
				streamNodeProperties.setHeapMemoryInMB(minResourceSpec.getHeapMemory());
				streamNodeProperties.setDirectMemoryInMB(minResourceSpec.getDirectMemory());
				streamNodeProperties.setNativeMemoryInMB(minResourceSpec.getNativeMemory());
				streamNodeProperties.setGpuLoad(minResourceSpec.getGPUResource());
				if (minResourceSpec.getExtendedResources().containsKey(ResourceSpec.MANAGED_MEMORY_NAME)) {
					streamNodeProperties.setManagedMemoryInMB(
							(int) ((minResourceSpec.getExtendedResources().get(ResourceSpec.MANAGED_MEMORY_NAME)).getValue()));

				}
			} else {
				streamNodeProperties.setCpuCores(resSettings.getDefaultOperatorCpuCores());
				streamNodeProperties.setHeapMemoryInMB(resSettings.getDefaultOperatorHeapMemoryMb());
			}

			MiniBatchUtil.trySetMiniBatchProperties(streamNodeProperties, streamNode);
			properties.getStreamNodeProperties().add(streamNodeProperties);
		}
		properties.getStatefulNodes().addAll(statefulNodes);
		LOG.info("statefulNodes: " + statefulNodes.toString());

		// links
		for (StreamNode source : streamGraph.getStreamNodes()) {
			int index = 0;
			for (StreamEdge edge : source.getOutEdges()) {
				StreamEdgeProperty streamEdgeProperty = new StreamEdgeProperty(
						edge.getSourceId(), edge.getTargetId(), index);
				streamEdgeProperty.setShipStrategy(edge.getPartitioner().toString());
				properties.getStreamEdgeProperties().add(streamEdgeProperty);
			}
		}

		properties.postUpdate();

		// set suitable parallelism according to source parallelism
		streamGraph.getStreamNodes().forEach(node ->
				extractInputPartitionSource(node.getOperator(),
					node.getId(), getNodeName(node.getOperatorName())));

		setInitialResourceConfig(properties, statefulNodes);

		return properties;
	}

	public StreamGraphProperty generateProperties(StreamGraph streamGraph, StreamGraphProperty oldProperty) {
		StreamGraphProperty property = generateProperties(streamGraph);
		if (oldProperty == null) {
			return property;
		}
		for (StreamNodeProperty nodeProperty : property.getStreamNodeProperties()) {
			if (oldProperty.getIdToNodeMap().containsKey(nodeProperty.getId())) {
				nodeProperty.update(oldProperty.getIdToNodeMap().get(nodeProperty.getId()));
			}
		}
		for (StreamEdgeProperty edgeProperty : property.getStreamEdgeProperties()) {
			if (oldProperty.getIdToEdgeMap().containsKey(edgeProperty.getSource()) &&
					oldProperty.getIdToEdgeMap().get(edgeProperty.getSource()).containsKey(edgeProperty.getTarget()) &&
						oldProperty.getIdToEdgeMap().get(edgeProperty.getSource()).get(edgeProperty.getTarget()).get(edgeProperty.getIndex()) != null) {
				edgeProperty.update(
						oldProperty.getIdToEdgeMap().get(edgeProperty.getSource()).get(edgeProperty.getTarget()).get(edgeProperty.getIndex()));
			}
		}
		property.postUpdate();
		return property;
	}

	/**
	 * Set initial resource config based on experience.
	 *
	 * @param transProperties StreamGraphProperty from stream graph
	 * @param statefulNodes ids of stateful node
	 */
	private void setInitialResourceConfig(StreamGraphProperty transProperties, Set<Integer> statefulNodes) {
		Map<Integer, Integer> sourceParallelismMap = new HashMap<>();

		SourcePartitionFetcher.getInstance().getSourcePartitions().forEach((k, v) ->
			sourceParallelismMap.put(k, InputTranslationUtil.calSuitableParallelism(v))
		);

		this.setInitParallelismAccordingToSource(transProperties, sourceParallelismMap);

		transProperties.postUpdate();

		// adjustment vertex memory
		Set<Integer> alreadySetNativeNodes = new HashSet<>();
		for (Collection<Integer> groups : transProperties.getChainedGroups()) {
			// avoid memory waste when has too many nodes in one vertex
			if (groups.size() >= 5) {
				adjustVertexHeapMemory(transProperties, resSettings, groups, 5);
			}

			// avoid source OOM
			if (getSourceThreadNums(transProperties, groups, sourceParallelismMap) >= 5) {
				adjustVertexHeapMemory(transProperties, resSettings, groups, 5);
			}

			// decrease memory when has state
			if (groups.size() >= 2 && hasStateNode(groups, statefulNodes)) {
				adjustVertexHeapMemory(transProperties, resSettings, groups, 2);
			}

			// add default native memory
			adjustVertexNativeMemory(transProperties, groups, statefulNodes);
			alreadySetNativeNodes.addAll(groups);
		}

		// set native memory when not set
		for (StreamNodeProperty streamNodeProperties : transProperties.getStreamNodeProperties()) {
			int id = streamNodeProperties.getId();
			if (!alreadySetNativeNodes.contains(id)) {
				if (statefulNodes.contains(id)) {
					streamNodeProperties.setNativeMemoryInMB(StateBackendUtil.getOperatorNativeMemory(userProperties, true));
				} else {
					streamNodeProperties.setNativeMemoryInMB(StateBackendUtil.getOperatorNativeMemory(userProperties, false));
					if (streamNodeProperties.getHeapMemoryInMB() > streamNodeProperties.getNativeMemoryInMB()) {
						streamNodeProperties.setHeapMemoryInMB(streamNodeProperties.getHeapMemoryInMB() - streamNodeProperties.getNativeMemoryInMB()); // avoid heap + native exceed cpu:mem ratio
					}
				}
			}
		}
	}

	private boolean hasStateNode(Collection<Integer> groups, Set<Integer> statefulNodes) {
		for (Integer id : groups) {
			if (statefulNodes.contains(id)) {
				return true;
			}
		}

		return false;
	}

	private void adjustVertexHeapMemory(StreamGraphProperty transformationProperties, ResourceSettings resourceSettings, Collection<Integer> groups, int resourceUnits) {
		if (groups.size() == 0) {
			return;
		}

		int heap = resourceSettings.getDefaultOperatorHeapMemoryMb() * resourceUnits / groups.size();
		for (Integer id : groups) {
			transformationProperties.getIdToNodeMap().get(id).setHeapMemoryInMB(heap);
		}
	}

	private void adjustVertexNativeMemory(StreamGraphProperty transformationProperties, Collection<Integer> groups, Set<Integer> statefulNodes) {
		if (groups.size() == 0) {
			return;
		}

		Collection<Integer> statefulNodesInGroup = new HashSet<>();
		for (Integer id : groups) {
			if (statefulNodes.contains(id)) {
				statefulNodesInGroup.add(id);
			}
		}

		int nativeMemPerNode;
		Collection<Integer> nodesToSetNative;
		if (statefulNodesInGroup.size() > 0) {
			nativeMemPerNode = StateBackendUtil.getOperatorNativeMemory(userProperties, true, statefulNodesInGroup.size()) / statefulNodesInGroup.size();
			nodesToSetNative = statefulNodesInGroup;
		} else {
			nativeMemPerNode = StateBackendUtil.getOperatorNativeMemory(userProperties, false) / groups.size();
			nodesToSetNative = groups;
		}

		for (Integer id : nodesToSetNative) {
			StreamNodeProperty streamNodeProperties = transformationProperties.getIdToNodeMap().get(id);
			streamNodeProperties.setNativeMemoryInMB(nativeMemPerNode);
			if (!statefulNodes.contains(id)) {
				if (streamNodeProperties.getHeapMemoryInMB() > streamNodeProperties.getNativeMemoryInMB()) {
					streamNodeProperties.setHeapMemoryInMB(streamNodeProperties.getHeapMemoryInMB() - streamNodeProperties.getNativeMemoryInMB()); // avoid heap + native exceed cpu:mem ratio
				}
			}
		}
	}

	private int getSourceThreadNums(StreamGraphProperty transformationProperties, Collection<Integer> groups, Map<Integer, Integer> sourcePartitioins) {
		int max = -1;
		for (Integer id : groups) {
			if (sourcePartitioins.containsKey(id)) {
				int parall = transformationProperties.getIdToNodeMap().get(id).getParallelism();
				if (parall > 0) {
					int num = (int) Math.ceil(1.0 * sourcePartitioins.get(id) / parall);
					if (num > max) {
						max = num;
					}
				}
			}
		}

		return max;
	}

	/**
	 * Setting parallelism according to parallelism of source.
	 * @param transProperties       stream graph property to setup
	 * @param sourceParallelismMap  source parallelism map
	 */
	private void setInitParallelismAccordingToSource(StreamGraphProperty transProperties, Map<Integer, Integer> sourceParallelismMap) {

		if (sourceParallelismMap.isEmpty()) {
			return;
		}

		int sourcePartitionSum = 0;
		Set<Integer> alreadySetNodes = new HashSet<>();
		// set source and parser the same parallel
		for (Integer source : sourceParallelismMap.keySet()) {
			StreamNodeProperty sourceStreamNodeProperties = transProperties.getIdToNodeMap().get(source);
			int sourceParallelism = sourceParallelismMap.get(source);
			sourceStreamNodeProperties.setParallelism(sourceParallelism);
			sourcePartitionSum += sourceParallelism;

			transProperties.getChainedGroups().stream()
				.filter(groups -> {
					if (groups.contains(source)) {
						return true;
					}
					return false;
				})
				.forEach((group -> group.forEach(id -> {
					StreamNodeProperty tempStreamNodeProperties = transProperties.getIdToNodeMap().get(id);
					if (tempStreamNodeProperties != null) {
						tempStreamNodeProperties.setParallelism(sourceParallelism);
					}
					alreadySetNodes.add(id);
				})));
		}
	}

	private void extractInputPartitionSource(StreamOperator<?> operator, int id, String name) {
		if (operator instanceof StreamSource) {
			final Function userFunction = ((StreamSource) operator).getUserFunction();
			if (userFunction instanceof InputPartitionSource) {
				InputPartitionSource source = (InputPartitionSource) userFunction;
				SourcePartitionFetcher.getInstance().addSourceFunction(id, name, source);
			}
		}
	}

	private String getNodeName(String nodeName) {
		String truncName;
		int nameMaxLength = resSettings.getOperatorNameMaxLength();
		if (nameMaxLength <= 0) {
			truncName = nodeName;
		} else {
			truncName = nodeName.length() > nameMaxLength
				? nodeName.substring(0, nameMaxLength)
				: nodeName;
		}

		return StringUtil.filterSpecChars(truncName.trim())
			.replaceFirst("Source: ", "") // "Source: " will be auto added
			.replaceFirst("Sink: ", ""); // "Sink: " will be auto added
	}

	/**
	 * Builder for RuleBasedStreamGraphPropertyGenerator.
	 */
	public static class Builder {
		private Properties userProperties = new Properties();

		public Builder setUserProperties(Properties userProperties) {
			this.userProperties = userProperties;
			return this;
		}

		public RuleBasedStreamGraphPropertyGenerator build() {
			RuleBasedStreamGraphPropertyGenerator jsonGenerator = new RuleBasedStreamGraphPropertyGenerator(userProperties);
			return jsonGenerator;
		}
	}
}
