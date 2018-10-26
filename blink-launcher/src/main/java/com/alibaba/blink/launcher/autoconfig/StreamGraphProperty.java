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

import org.apache.flink.annotation.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Properties for StreamGraph.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonIgnoreProperties(value = {
		"global", "resourceSettings", "autoConfig", "parameters", "vertexAdjustments",
		"vertices", "workers", "subDags"})
public class StreamGraphProperty extends AbstractJsonSerializable {

	@JsonProperty("nodes")
	private TreeSet<StreamNodeProperty> streamNodeProperties = new TreeSet<>();

	@JsonProperty("links")
	private TreeSet<StreamEdgeProperty> streamEdgeProperties = new TreeSet<>();

	@JsonProperty("statefulNodes")
	private TreeSet<Integer> statefulNodes = new TreeSet<>();

	@JsonIgnore
	private Map<Integer, StreamNodeProperty> idToNodeMap = new HashMap<>();
	@JsonIgnore
	private Map<Integer, Map<Integer, Map<Integer, StreamEdgeProperty>>> idToEdgeMap = new HashMap<>();
	@JsonIgnore
	private Map<Integer, List<Integer>> sourceTargetMap = new TreeMap<>();
	@JsonIgnore
	private Map<Integer, List<Integer>> targetSourceMap = new TreeMap<>();
	@JsonIgnore
	private Map<String, String> shipStrategyMap = new HashMap<>();
	@JsonIgnore
	private List<Collection<Integer>> chainedGroups = new ArrayList<Collection<Integer>>();

	public StreamGraphProperty() {
	}

	@VisibleForTesting
	public static StreamGraphProperty fromJson(String json) {
		StreamGraphProperty streamGraphProperty = fromJson(json, StreamGraphProperty.class);
		if (streamGraphProperty != null) {
			streamGraphProperty.postUpdate();
		}
		return streamGraphProperty;
	}

	@VisibleForTesting
	public static StreamGraphProperty fromJson(File file) {
		StreamGraphProperty streamGraphProperty = fromJson(file, StreamGraphProperty.class);
		if (streamGraphProperty != null) {
			streamGraphProperty.postUpdate();
		}
		return streamGraphProperty;
	}

	/**
	 * Update the derivative properties after base properties are updated.
	 */
	public void postUpdate() {

		if (streamNodeProperties.isEmpty()) {
			return;
		}

		idToNodeMap = streamNodeProperties.stream().collect(Collectors.toMap(StreamNodeProperty::getId, streamNodeProperties -> streamNodeProperties));

		idToEdgeMap = new HashMap<>();
		streamEdgeProperties.stream().forEach(
				property -> idToEdgeMap
						.computeIfAbsent(property.getSource(), p -> new HashMap<>())
						.computeIfAbsent(property.getTarget(), p -> new HashMap<>())
						.put(property.getIndex(), property)
		);

		shipStrategyMap = streamEdgeProperties.stream().collect(
			Collectors.toMap(streamEdgeProperty -> (
					streamEdgeProperty.getSource() + "_" + streamEdgeProperty.getTarget() + "_" + streamEdgeProperty.getIndex()), StreamEdgeProperty::getShipStrategy));

		sourceTargetMap = streamEdgeProperties.stream().collect(Collectors.groupingBy(
			StreamEdgeProperty::getSource,
			Collectors.mapping(StreamEdgeProperty::getTarget, Collectors.toList())));

		targetSourceMap = streamEdgeProperties.stream().collect(Collectors.groupingBy(
			StreamEdgeProperty::getTarget,
			Collectors.mapping(StreamEdgeProperty::getSource, Collectors.toList())));

		chainedGroups = new OperatorChainCalculator(this, false).getChainedGroups();

	}

	public TreeSet<StreamNodeProperty> getStreamNodeProperties() {
		return streamNodeProperties;
	}

	public TreeSet<StreamEdgeProperty> getStreamEdgeProperties() {
		return streamEdgeProperties;
	}

	public TreeSet<Integer> getStatefulNodes() {
		return statefulNodes;
	}

	public Map<Integer, StreamNodeProperty> getIdToNodeMap() {
		return idToNodeMap;
	}

	public Map<Integer, Map<Integer, Map<Integer, StreamEdgeProperty>>> getIdToEdgeMap() {
		return idToEdgeMap;
	}

	public List<Collection<Integer>> getChainedGroups() {
		return chainedGroups;
	}

	public Map<Integer, List<Integer>> getSourceTargetMap() {
		return sourceTargetMap;
	}

	public Map<Integer, List<Integer>> getTargetSourceMap() {
		return targetSourceMap;
	}

	public Map<String, String> getShipStrategyMap() {
		return shipStrategyMap;
	}

	/**
	 * Whether two streamNodeProperties is same slot sharing group.
	 *
	 * @param sourceId Source id
	 * @param targetId Target id
	 * @return True when is same slot sharing group, otherwise false
	 */
	public boolean isSameSlotSharingGroup(Integer sourceId, Integer targetId) {
		return idToNodeMap.containsKey(sourceId) && idToNodeMap.containsKey(targetId) &&
			Objects.equals(
				idToNodeMap.get(sourceId).getSlotSharingGroup(),
				idToNodeMap.get(targetId).getSlotSharingGroup());
	}

	/**
	 * Whether the link between two streamNodeProperties is forward partitioner.
	 *
	 * @param sourceId Source id
	 * @param targetId Target id
	 * @return Tren when is forward partition, otherwise false
	 */
	public boolean isForwardPartitioner(Integer sourceId, Integer targetId, Integer index) {
		String strategy = shipStrategyMap.get(sourceId + "_" + targetId + "_" + index);
		return (strategy != null && strategy.equals("FORWARD"));
	}

}
