/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.FormatUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.MultiInputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorEdgeDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.ArbitraryInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	/**
	 * Restart delay used for the FixedDelayRestartStrategy in case checkpointing was enabled but
	 * no restart strategy has been specified.
	 */
	private static final long DEFAULT_RESTART_DELAY = 0L;

	/**
	 * Maps job vertex id to stream node ids.
	 */
	public static final String JOB_VERTEX_TO_STREAM_NODE_MAP = "jobVertexToStreamNodeMap";

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return new StreamingJobGraphGenerator(streamGraph).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final JobGraph jobGraph;

	/**
	 * The mapping of chained node to JobVertex.
	 */
	private final Map<Integer, JobVertex> nodeToJobVertexMap;

	/**
	 * The output edge list of all chains which is global sorted by depth-first.
	 */
	private final List<StreamEdge> transitiveOutEdges;

	/**
	 * The mapping of starting traversal head node to all nodes of chain.
	 */
	private final Map<Integer, List<Integer>> chainedNodeIdsMap;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Collections.singletonList(new StreamGraphUserHashHasher());

		this.nodeToJobVertexMap = new HashMap<>();
		this.transitiveOutEdges = new ArrayList<>();
		this.chainedNodeIdsMap = new HashMap<>();

		this.jobGraph = new JobGraph(streamGraph.getJobName());
	}

	private JobGraph createJobGraph() {

		// add custom configuration to the job graph
		jobGraph.addCustomConfiguration(streamGraph.getCustomConfiguration());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		setChaining(hashes, legacyHashes);

		connectEdges();

		setSlotSharing();

		configureCheckpointing();

		setSchedulerConfiguration();

		// add registered cache file into job configuration
		for (Tuple2<String, DistributedCache.DistributedCacheEntry> e : streamGraph.getCachedFiles()) {
			jobGraph.addUserArtifact(e.f0, e.f1);
		}

		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	/**
	 * Set parameters for job scheduling. Schedulers may leverage these parameters to schedule tasks.
	 */
	private void setSchedulerConfiguration() {
		Configuration configuration = jobGraph.getSchedulingConfiguration();

		setVertexToStreamNodesMap(configuration);
		configuration.addAll(streamGraph.getCustomConfiguration());
	}

	private void setVertexToStreamNodesMap(Configuration configuration) {
		Map<JobVertexID, List<Integer>> vertexToStreamNodeIds = new HashMap<>();
		for (Map.Entry<Integer, List<Integer>> entry : chainedNodeIdsMap.entrySet()) {
			JobVertex jobVertex = nodeToJobVertexMap.get(entry.getKey());
			vertexToStreamNodeIds.put(jobVertex.getID(), entry.getValue() == null ? Collections.emptyList() : entry.getValue());
		}

		try {
			InstantiationUtil.writeObjectToConfig(vertexToStreamNodeIds, configuration, JOB_VERTEX_TO_STREAM_NODE_MAP);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Could not serialize job vertex to stream node map", e);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
		final Map<Integer, ChainingStreamNode> chainingNodeMap = new HashMap<>(); // key: nodeId
		final Map<Integer, List<ChainingStreamNode>> chainingLayerMap = new HashMap<>(); // key: layerNumber

		final Map<Integer, ChainingStreamNode> chainedNodeMap;

		/**
		 *  Sorts source for the following purpose:
		 *  1.Processes StreamSource prior to StreamSourceV2.
		 *  2.Stabilizes the topology of job graph.
		 */
		List<Integer> sortedSourceIDs = streamGraph.getSourceIDs().stream()
			.sorted(
				Comparator.comparing((Integer id) -> {
					StreamOperator<?> operator = streamGraph.getStreamNode(id).getOperator();
					return operator == null || operator instanceof StreamSource ? 0 : 1;
				}).thenComparingInt(id -> id))
			.collect(Collectors.toList());

		// layer nodes according to input dependence using depth-first traversal
		int currentLayerNumber = 0;
		SequenceGenerator depthFirstSequenceGenerator = new SequenceGenerator();
		final Map<Integer, Integer> unvisitedInEdgeNumMap = new HashMap<>(); // key: nodeId
		for (Integer sourceNodeId : sortedSourceIDs) {
			layerNodes(sourceNodeId,
					currentLayerNumber,
					null,
					depthFirstSequenceGenerator,
					unvisitedInEdgeNumMap,
					chainingNodeMap,
					chainingLayerMap);
		}
		unvisitedInEdgeNumMap.clear();

		// split chains according to the specified strategy using breadth-first traversal
		if (streamGraph.isChainingEnabled()) {
			splitChain(chainingLayerMap, chainingNodeMap);
			chainedNodeMap = Collections.unmodifiableMap(chainingNodeMap);
		} else {
			chainedNodeMap = null;
		}

		// create chains
		ChainCreationStorager storager = new ChainCreationStorager();
		for (int i = 0; i < chainingLayerMap.size(); i++) {
			for (ChainingStreamNode chainingNode : chainingLayerMap.get(i)) {
				Integer startNodeId = chainingNode.getNodeId();

				if (createChain(startNodeId, startNodeId, new SequenceGenerator(), chainedNodeMap, hashes, legacyHashes, storager)) {
					for (Integer nodeId : storager.chainedNodeIdsInOrder) {
						nodeToJobVertexMap.put(nodeId, storager.createdVertex);
					}
					transitiveOutEdges.addAll(storager.chainOutEdgesInOrder);
					chainedNodeIdsMap.put(startNodeId, new ArrayList<>(storager.chainedNodeIdsInOrder));

					// add the job vertex to JobGraph
					jobGraph.addVertex(storager.createdVertex);
				}

				storager.clearChain();
			}
		}

		// Sorts output edges of all chains using depth-first.
		// The sorting policy must be consistent with {@code ChainCreationStorager.chainInEdgesInOrder}
		// and {@code ChainCreationStorager.chainOutEdgesInOrder} .
		transitiveOutEdges.sort(
			Comparator.comparingInt((StreamEdge o) -> chainingNodeMap.get(o.getTargetId()).getDepthFirstNumber())
				.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
		);
	}

	private int layerNodes(
			Integer currentNodeId,
			int currentLayerNumber,
			@Nullable Integer upstreamNodeId,
			SequenceGenerator depthFirstSequenceGenerator,
			Map<Integer, Integer> unvisitedInEdgeNumMap,
			Map<Integer, ChainingStreamNode> chainingNodeMap,
			Map<Integer, List<ChainingStreamNode>> chainingLayerMap) {

		StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

		Integer unvisitedInEdgeNum = -1;
		if (upstreamNodeId == null) {
			// the current node must be a zero-input head node
			unvisitedInEdgeNum = currentNode.getInEdges().size();

			checkState(unvisitedInEdgeNum == 0);
			unvisitedInEdgeNumMap.put(currentNodeId, unvisitedInEdgeNum);
		} else {
			Integer upstreamUnvisitedInEdgeNum = unvisitedInEdgeNumMap.get(upstreamNodeId);
			if (upstreamUnvisitedInEdgeNum != null && upstreamUnvisitedInEdgeNum == 0) {
				unvisitedInEdgeNum = unvisitedInEdgeNumMap.get(currentNodeId);
				if (unvisitedInEdgeNum == null) {
					unvisitedInEdgeNum = currentNode.getInEdges().size();
				}

				checkState(unvisitedInEdgeNum > 0);
				unvisitedInEdgeNumMap.put(currentNodeId, --unvisitedInEdgeNum);
			}
		}

		// traverse recursively
		int maxLayerNumber = currentLayerNumber;
		for (StreamEdge outEdge : currentNode.getOutEdges()) {
			int layerNumber = layerNodes(outEdge.getTargetId(),
					currentLayerNumber + 1,
					currentNodeId,
					depthFirstSequenceGenerator,
					unvisitedInEdgeNumMap,
					chainingNodeMap,
					chainingLayerMap);

			maxLayerNumber = Math.max(maxLayerNumber, layerNumber);
		}

		// create a corresponding ChainingStreamNode for the current node
		ChainingStreamNode currentChainingNode = chainingNodeMap.get(currentNodeId);
		if (currentChainingNode == null) {
			currentChainingNode = new ChainingStreamNode(currentNodeId,
					depthFirstSequenceGenerator.get(),
					currentNode.getInEdges().size());

			chainingNodeMap.put(currentNodeId, currentChainingNode);
		}

		// update the layer number of the current node
		currentChainingNode.updateLayer(currentLayerNumber);

		// add the current node to the layered list
		if (unvisitedInEdgeNum == 0) {
			chainingLayerMap.computeIfAbsent(currentChainingNode.getLayer(), k -> new ArrayList<>())
					.add(currentChainingNode);
		}

		return maxLayerNumber;
	}

	private void splitChain(Map<Integer, List<ChainingStreamNode>> chainingLayerMap, Map<Integer, ChainingStreamNode> chainingNodeMap) {
		SequenceGenerator breadthFirstSequenceGenerator = new SequenceGenerator();
		for (int i = 0; i < chainingLayerMap.size(); i++) {
			for (ChainingStreamNode currentChainingNode : chainingLayerMap.get(i)) {
				currentChainingNode.setBreadthFirstNumber(breadthFirstSequenceGenerator.get());

				if (currentChainingNode.getLayer() == 0) {
					StreamOperator<?> operator = streamGraph.getStreamNode(currentChainingNode.getNodeId()).getOperator();
					currentChainingNode.setAllowMultiHeadChaining(
							operator == null || operator instanceof StreamSource ? Boolean.FALSE : Boolean.TRUE);
				}

				StreamNode currentNode = streamGraph.getStreamNode(currentChainingNode.getNodeId());
				for (StreamEdge edge : currentNode.getOutEdges()) {
					ChainingStreamNode downstreamChainingNode = chainingNodeMap.get(edge.getTargetId());

					downstreamChainingNode.chainTo(currentChainingNode,
							edge,
							streamGraph.getStreamNode(edge.getSourceId()),
							streamGraph.getStreamNode(edge.getTargetId()),
							streamGraph.isMultiHeadChainMode(),
							streamGraph.isChainEagerlyEnabled());
				}
			}
		}

		/*
		   Checks cycles (treated as a undirected graph) and break off them in multi-head chaining mode
		   for the following purpose:
		   1. No cycle when connecting edges of the job graph.
		   2. No deadlock occurs when dynamic selection reading,
		      see {@code org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processRecord1(StreamRecord)}
		      and {@code org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processRecord2(StreamRecord)}.
		 */
		if (streamGraph.isMultiHeadChainMode() && chainingLayerMap.size() > 0) {
			Map<Integer, Integer> colorMap = new HashMap<>(); // key: nodeId, value: colorType (1, 2)
			Map<Integer, Integer> partMap = new HashMap<>(); // key: nodeId, value: prevNodeId

			Map<Integer, Set<Integer>> markMap = new HashMap<>(); // key: cycleNumber, value: nodeIds

			int cycleNumber = 0;
			for (ChainingStreamNode chainingNode : chainingLayerMap.get(0)) {
				int nodeId = chainingNode.getNodeId();
				if (colorMap.containsKey(nodeId)) {
					continue;
				}

				// call DFS to mark the cycles
				cycleNumber = markUndirectedGraphCycles(nodeId, null, colorMap, partMap, markMap, cycleNumber);
			}

			if (LOG.isDebugEnabled()) {
				for (int i = 1; i <= markMap.size(); i++) {
					LOG.debug("Found cycle " + i + " (treated as a undirected graph): " + markMap.get(i));
				}
			}

			breakOffUndirectedGraphCycles(markMap, chainingNodeMap);
		}
	}

	private int markUndirectedGraphCycles(
		Integer nodeId,
		Integer prevNodeId,
		Map<Integer, Integer> colorMap,
		Map<Integer, Integer> partMap,
		Map<Integer, Set<Integer>> markMap,
		int cycleNumber) {

		int color = colorMap.getOrDefault(nodeId, 0);
		if (color == 2) {
			// completely visited vertex.

			return cycleNumber;
		} else if (color == 1) {
			// seen vertex, but was not completely visited -> cycle detected.
			// backtrack based on parents to find the complete cycle.

			cycleNumber++;

			Integer currentNodeId = prevNodeId;
			markMap.computeIfAbsent(cycleNumber, k -> new HashSet<>()).add(currentNodeId);

			// backtrack the vertex which are
			// in the current cycle thats found
			while (!currentNodeId.equals(nodeId)) {
				currentNodeId = partMap.get(currentNodeId);
				markMap.computeIfAbsent(cycleNumber, k -> new HashSet<>()).add(currentNodeId);
			}

			return cycleNumber;
		}

		partMap.put(nodeId, prevNodeId);

		// partially visited.
		colorMap.put(nodeId, 1);

		// dfs on graph
		StreamNode node = streamGraph.getStreamNode(nodeId);
		for (List<StreamEdge> edgeList : new List[] {node.getInEdges(), node.getOutEdges()}) {
			for (StreamEdge edge : edgeList) {
				int sourceNodeId = edge.getSourceId();
				Integer nextNodeId = (sourceNodeId == nodeId) ? edge.getTargetId() : sourceNodeId;

				// if it has not been visited previously
				if (partMap.containsKey(nodeId) && nextNodeId.equals(partMap.get(nodeId))) {
					continue;
				}
				cycleNumber = markUndirectedGraphCycles(nextNodeId, nodeId, colorMap, partMap, markMap, cycleNumber);
			}
		}

		// completely visited.
		colorMap.put(nodeId, 2);
		return cycleNumber;
	}

	private void breakOffUndirectedGraphCycles(
		Map<Integer, Set<Integer>> markMap,
		Map<Integer, ChainingStreamNode> chainingNodeMap) {

		for (int i = 1; i <= markMap.size(); i++) {
			Integer breakingOffNodeId = null;
			int breakingOffNodeBFNumber = -1;

			Set<Integer> cycleNodes = markMap.get(i);
			for (Integer nodeId : cycleNodes) {
				int nodeBFNumber = chainingNodeMap.get(nodeId).getBreadthFirstNumber();
				if (breakingOffNodeId == null || breakingOffNodeBFNumber < nodeBFNumber) {
					breakingOffNodeId = nodeId;
					breakingOffNodeBFNumber = nodeBFNumber;
				}
			}

			StreamNode breakingOffNode = streamGraph.getStreamNode(breakingOffNodeId);
			if (breakingOffNode.getInEdges().size() < 2) {
				throw new RuntimeException("The stream graph is cyclic.");
			}
			for (StreamEdge inEdge : breakingOffNode.getInEdges()) {
				if (!isInputEdgeBrokenOff(inEdge, breakingOffNodeId, cycleNodes, chainingNodeMap)) {
					ChainingStreamNode chainingNode = chainingNodeMap.get(breakingOffNodeId);
					chainingNode.removeChainableToNode(inEdge.getSourceId());
				}
			}
		}
	}

	private boolean isInputEdgeBrokenOff(
		final StreamEdge edge,
		final Integer breakingOffNodeId,
		Set<Integer> cycleNodes,
		Map<Integer, ChainingStreamNode> chainingNodeMap) {

		Integer sourceId = edge.getSourceId();
		if (sourceId.equals(breakingOffNodeId)) {
			throw new RuntimeException("The stream graph is cyclic.");
		}

		if (!cycleNodes.contains(sourceId)) {
			return false;
		}

		ChainingStreamNode targetChainingNode = chainingNodeMap.get(edge.getTargetId());
		if (!targetChainingNode.isChainTo(sourceId)) {
			return true;
		}

		for (StreamEdge nextEdge : streamGraph.getStreamNode(sourceId).getInEdges()) {
			if (isInputEdgeBrokenOff(nextEdge, breakingOffNodeId, cycleNodes, chainingNodeMap)) {
				return true;
			}
		}

		return false;
	}

	private boolean createChain(
			Integer startNodeId,
			Integer currentNodeId,
			SequenceGenerator chainIndexGenerator,
			@Nullable Map<Integer, ChainingStreamNode> chainedNodeMap,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			ChainCreationStorager storager) {

		if (storager.allBuiltNodes.contains(currentNodeId)) {
			return false;
		}

		storager.allBuiltNodes.add(currentNodeId);

		// current node related
		StreamNode currentStreamNode = streamGraph.getStreamNode(currentNodeId);

		int chainIndex = chainIndexGenerator.get();
		byte[] primaryHashBytes = hashes.get(currentNodeId);
		boolean isHeadNode = (chainedNodeMap == null || chainedNodeMap.get(currentNodeId).isChainHeadNode());

		List<StreamEdge> chainedOutputs = new ArrayList<>();
		List<StreamEdge> nonChainedOutputs = new ArrayList<>();

		/* Traverses from the current node, first going down and up. */

		// going down
		for (StreamEdge outEdge : currentStreamNode.getOutEdges()) {
			Integer downstreamNodeId = outEdge.getTargetId();
			ChainingStreamNode downstreamNode = (chainedNodeMap == null) ? null : chainedNodeMap.get(downstreamNodeId);

			if (chainedNodeMap == null || !downstreamNode.isChainTo(currentNodeId)) {
				nonChainedOutputs.add(outEdge);
				continue;
			}

			chainedOutputs.add(outEdge);

			createChain(
				startNodeId,
				downstreamNodeId,
				chainIndexGenerator,
				chainedNodeMap,
				hashes,
				legacyHashes,
				storager);
		}

		// generate chained name of the current node
		storager.chainedNameMap.put(currentNodeId, makeChainedName(currentStreamNode.getOperatorName(), chainedOutputs, storager.chainedNameMap));

		// going up
		if (chainedNodeMap != null) {
			for (StreamEdge inEdge : currentStreamNode.getInEdges()) {
				Integer upstreamNodeId = inEdge.getSourceId();
				ChainingStreamNode currentNode = chainedNodeMap.get(currentNodeId);
				if (!currentNode.isChainTo(upstreamNodeId)) {
					continue;
				}

				createChain(
					startNodeId,
					upstreamNodeId,
					chainIndexGenerator,
					chainedNodeMap,
					hashes,
					legacyHashes,
					storager);
					}
		}

		/* The traversal is finished. */

		// create StreamConfig for the current node
		StreamConfig currentNodeConfig = new StreamConfig(new Configuration());
		OperatorID currentOperatorID = new OperatorID(primaryHashBytes);

		if (isHeadNode) {
			currentNodeConfig.setChainStart();
		}
		currentNodeConfig.setChainIndex(chainIndex);
		currentNodeConfig.setOperatorName(currentStreamNode.getOperatorName());
		currentNodeConfig.setOperatorID(currentOperatorID);
		if (chainedOutputs.isEmpty()) {
			currentNodeConfig.setChainEnd();
		}

		List<StreamEdge> nonChainedInputs = new ArrayList<>();
		for (StreamEdge inEdge : currentStreamNode.getInEdges()) {
			if (chainedNodeMap == null) {
				nonChainedInputs.add(inEdge);
			} else {
				ChainingStreamNode currentNode = chainedNodeMap.get(currentNodeId);
				if (!currentNode.isChainTo(inEdge.getSourceId())) {
					nonChainedInputs.add(inEdge);
				}
			}
		}

		setupNodeConfig(currentNodeId, nonChainedInputs, chainedOutputs, nonChainedOutputs, streamGraph, currentNodeConfig);

		// compute and store chained data
		storager.chainedConfigMap.put(currentNodeId, currentNodeConfig);
		storager.chainedNodeIdsInOrder.add(currentNodeId);
		if (isHeadNode) {
			storager.chainedHeadNodeIdsInOrder.add(currentNodeId);
		}

		storager.chainInEdgesInOrder.addAll(nonChainedInputs);
		storager.chainOutEdgesInOrder.addAll(nonChainedOutputs);

		if (currentStreamNode.getOutputFormat() != null) {
			storager.chainOutputFormatMap.put(currentOperatorID, currentStreamNode.getOutputFormat());
		}
		if (currentStreamNode.getInputFormat() != null) {
			storager.chainInputFormatMap.put(currentOperatorID, currentStreamNode.getInputFormat());
		}

		ResourceSpec currentNodeMinResources = currentStreamNode.getMinResources();
		storager.chainedMinResources = (storager.chainedMinResources == null) ?
			currentNodeMinResources : storager.chainedMinResources.merge(currentNodeMinResources);

		ResourceSpec currentNodePreferredResources = currentStreamNode.getPreferredResources();
		storager.chainedPreferredResources = (storager.chainedPreferredResources == null) ?
			currentNodePreferredResources : storager.chainedPreferredResources.merge(currentNodePreferredResources);

		// The chain is end, create job vertex and configuration.
		if (currentNodeId.equals(startNodeId)) {
			if (chainedNodeMap != null) {
				// sort related lists
				storager.chainInEdgesInOrder.sort(
					Comparator.comparingInt((StreamEdge o) -> chainedNodeMap.get(o.getTargetId()).getDepthFirstNumber())
						.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
				);
				storager.chainOutEdgesInOrder.sort(
					Comparator.comparingInt((StreamEdge o) -> chainedNodeMap.get(o.getTargetId()).getDepthFirstNumber())
						.thenComparingInt((o) -> streamGraph.getStreamNode(o.getTargetId()).getInEdges().indexOf(o))
				);

				storager.chainedNodeIdsInOrder.sort(Comparator.comparingInt((o) -> chainedNodeMap.get(o).getDepthFirstNumber()));
				storager.chainedHeadNodeIdsInOrder.sort(Comparator.comparingInt((o) -> chainedNodeMap.get(o).getBreadthFirstNumber()));
			}

			storager.createdVertex = createJobVertex(startNodeId, hashes, legacyHashes, storager);

			setupVertexConfig(currentNodeConfig, storager, storager.createdVertex.getConfiguration());
		}

		return true;
	}

	private JobVertex createJobVertex(
			Integer startNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			ChainCreationStorager storager) {

		JobVertex jobVertex;

		// generate the id of the job vertex
		byte[] primaryHashBytes = hashes.get(startNodeId);
		if (primaryHashBytes == null) {
			throw new IllegalStateException("Cannot find node hash (nodeId: " + startNodeId + ") . " +
					"Did you generate them before calling this method?");
		}
		JobVertexID jobVertexId = new JobVertexID(primaryHashBytes);

		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			byte[] hash = legacyHash.get(startNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		// generate id for chained operators
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();

		for (Integer nodeId : storager.chainedNodeIdsInOrder) {
			byte[] hash = hashes.get(nodeId);
			for (Map<Integer, byte[]> legacyHashMap : legacyHashes) {
				chainedOperatorVertexIds.add(new OperatorID(hash));

				byte[] legacyHash = legacyHashMap.get(nodeId);
				userDefinedChainedOperatorVertexIds.add(legacyHash != null ? new OperatorID(legacyHash) : null);
			}
		}

		// create job vertex
		String jobVertexName = makeJobVertexName(storager.chainedHeadNodeIdsInOrder, storager.chainedNameMap);

		StreamNode startStreamNode = streamGraph.getStreamNode(startNodeId);
		if (storager.chainInputFormatMap.size() != 0 || storager.chainOutputFormatMap.size() != 0) {
			jobVertex = new MultiInputOutputFormatVertex(
				jobVertexName,
				jobVertexId,
				legacyJobVertexIds,
				chainedOperatorVertexIds,
				userDefinedChainedOperatorVertexIds);

			TaskConfig taskConfig = new TaskConfig(jobVertex.getConfiguration());
			FormatUtil.MultiFormatStub.setStubFormats(
				taskConfig,
				storager.chainInputFormatMap.size() == 0 ? null : storager.chainInputFormatMap,
				storager.chainOutputFormatMap.size() == 0 ? null : storager.chainOutputFormatMap);
		} else {
			jobVertex = new JobVertex(
					jobVertexName,
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		for (Integer nodeId : storager.chainedNodeIdsInOrder) {
			final byte[] hash = hashes.get(nodeId);
			final StreamNode node = streamGraph.getStreamNode(nodeId);
			final OperatorID operatorID = new OperatorID(hash);
			final OperatorDescriptor operatorDescriptor = new OperatorDescriptor(node.getOperatorName(), operatorID);
			for (StreamEdge streamEdge : node.getInEdges()) {
				final OperatorEdgeDescriptor edgeDescriptor = new OperatorEdgeDescriptor(
					new OperatorID(hashes.get(streamEdge.getSourceId())),
					operatorID,
					streamEdge.getTypeNumber(),
					streamEdge.getPartitioner() == null ? "null" : streamEdge.getPartitioner().toString());
				operatorDescriptor.addInput(edgeDescriptor);
			}
			jobVertex.addOperatorDescriptor(operatorDescriptor);
		}

		// set properties of job vertex
		jobVertex.setResources(storager.chainedMinResources, storager.chainedPreferredResources);
		if (storager.chainedHeadNodeIdsInOrder.size() < 2) {
			jobVertex.setInvokableClass(startStreamNode.getJobVertexClass());
		} else {
			jobVertex.setInvokableClass(ArbitraryInputStreamTask.class);
		}

		int parallelism = startStreamNode.getParallelism();
		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, startNodeId);
		}

		jobVertex.setMaxParallelism(startStreamNode.getMaxParallelism());

		return jobVertex;
	}

	private void setupVertexConfig(StreamConfig anyheadNodeConfig, ChainCreationStorager storager, Configuration config) {
		StreamTaskConfigCache configCache = storager.vertexConfigCache;

		configCache.setTimeCharacteristic(anyheadNodeConfig.getTimeCharacteristic());
		configCache.setCheckpointingEnabled(anyheadNodeConfig.isCheckpointingEnabled());
		configCache.setCheckpointMode(anyheadNodeConfig.getCheckpointMode());
		configCache.setStateBackend(anyheadNodeConfig.getStateBackend(storager.classLoader));

		configCache.setChainedNodeConfigs(storager.chainedConfigMap);
		configCache.setChainedHeadNodeIds(storager.chainedHeadNodeIdsInOrder);
		configCache.setInStreamEdgesOfChain(storager.chainInEdgesInOrder);
		configCache.setOutStreamEdgesOfChain(storager.chainOutEdgesInOrder);

		configCache.serializeTo(new StreamTaskConfig(config));
	}

	private static void setupNodeConfig(Integer nodeId,
		List<StreamEdge> nonChainableInputs,
		List<StreamEdge> chainableOutputs,
		List<StreamEdge> nonChainableOutputs,
		StreamGraph streamGraph,
		StreamConfig config) {

		StreamNode vertex = streamGraph.getStreamNode(nodeId);

		config.setVertexID(nodeId);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfInputs(nonChainableInputs.size());
		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig ceckpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(ceckpointCfg.isCheckpointingEnabled());
		if (ceckpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(ceckpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(nodeId));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(nodeId));
		}

		Configuration customConfiguration = vertex.getCustomConfiguration();
		if (customConfiguration.keySet().size() > 0) {
			config.setCustomConfiguration(customConfiguration);
		}
	}

	private static String makeChainedName(String currentOperatorName, List<StreamEdge> chainedOutputs, Map<Integer, String> chainedNames) {
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return currentOperatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return currentOperatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return currentOperatorName;
		}
	}

	private static String makeJobVertexName(List<Integer> sortedHeadNodeIds, Map<Integer, String> chainedNames) {
		StringBuilder nameBuffer = new StringBuilder();

		int chainedHeadNodeCount = sortedHeadNodeIds.size();
		if (chainedHeadNodeCount > 1) {
			nameBuffer.append("[");
		}
		for (int i = 0; i < chainedHeadNodeCount; i++) {
			if (i > 0) {
				nameBuffer.append(", ");
			}
			nameBuffer.append(chainedNames.get(sortedHeadNodeIds.get(i)));
		}
		if (chainedHeadNodeCount > 1) {
			nameBuffer.append("]");
		}
		return nameBuffer.toString();
	}

	private ResultPartitionType getEdgeResultPartitionType(DataExchangeMode dataExchangeMode) {
		switch (dataExchangeMode) {
			case AUTO:
				switch (streamGraph.getExecutionConfig().getExecutionMode()) {
					case PIPELINED:
						return ResultPartitionType.PIPELINED;
					case BATCH:
						return ResultPartitionType.BLOCKING;
					default:
						throw new UnsupportedOperationException("Unknown execution mode " +
							streamGraph.getExecutionConfig().getExecutionMode() + ".");
				}
			case PIPELINED:
				return ResultPartitionType.PIPELINED;
			case BATCH:
				return ResultPartitionType.BLOCKING;
			case PIPELINE_WITH_BATCH_FALLBACK:
				throw new UnsupportedOperationException("Data exchange mode " +
					dataExchangeMode + " is not supported.");
			default:
				throw new UnsupportedOperationException("Unknown data exchange mode " + dataExchangeMode + ".");
		}
	}

	private void connectEdges() {
		for (StreamEdge edge : transitiveOutEdges) {
			JobVertex upstreamVertex = nodeToJobVertexMap.get(edge.getSourceId());
			JobVertex downstreamVertex = nodeToJobVertexMap.get(edge.getTargetId());

			if (upstreamVertex.getID().equals(downstreamVertex.getID())) {
				throw new RuntimeException("The job graph is cyclic.");
			}

			StreamPartitioner<?> partitioner = edge.getPartitioner();
			IntermediateDataSetID dataSetID = new IntermediateDataSetID(edge.getEdgeID());
			JobEdge jobEdge;
			if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
				jobEdge = downstreamVertex.connectDataSetAsInput(
					upstreamVertex,
					dataSetID,
					DistributionPattern.POINTWISE,
					getEdgeResultPartitionType(edge.getDataExchangeMode()));
			} else {
				jobEdge = downstreamVertex.connectDataSetAsInput(
					upstreamVertex,
					dataSetID,
					DistributionPattern.ALL_TO_ALL,
					getEdgeResultPartitionType(edge.getDataExchangeMode()));
			}
			// set strategy name so that web interface can show it.
			jobEdge.setShipStrategyName(partitioner.toString());

			if (LOG.isDebugEnabled()) {
				LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					edge.getSourceId(), edge.getTargetId());
			}
		}
	}

	private void setSlotSharing() {

		Map<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();

		for (Integer startHeadNodeId : chainedNodeIdsMap.keySet()) {
			JobVertex vertex = nodeToJobVertexMap.get(startHeadNodeId);
			String slotSharingGroup = streamGraph.getStreamNode(startHeadNodeId).getSlotSharingGroup();

			SlotSharingGroup group = slotSharingGroups.get(slotSharingGroup);
			if (group == null) {
				group = new SlotSharingGroup();
				slotSharingGroups.put(slotSharingGroup, group);
			}
			vertex.setSlotSharingGroup(group);
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = nodeToJobVertexMap.get(pair.f0.getId());
			JobVertex sink = nodeToJobVertexMap.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}

	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval > 0) {

			ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
			// propagate the expected behaviour for checkpoint errors to task.
			executionConfig.setFailTaskOnCheckpointError(cfg.isFailOnCheckpointingErrors());

			// check if a restart strategy has been set, if not then set the FixedDelayRestartStrategy
			if (executionConfig.getRestartStrategy() == null) {
				// if the user enabled checkpointing, the default number of exec retries is infinite.
				executionConfig.setRestartStrategy(
					RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, DEFAULT_RESTART_DELAY));
			}
		} else {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		List<JobVertexID> ackVertices = new ArrayList<>(chainedNodeIdsMap.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		List<JobVertexID> commitVertices = new ArrayList<>(chainedNodeIdsMap.size());

		for (Integer startHeadNodeId : chainedNodeIdsMap.keySet()) {
			JobVertex vertex = nodeToJobVertexMap.get(startHeadNodeId);
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			StreamOperator<?> op = node.getOperator();
			if (op instanceof AbstractUdfStreamOperator) {
				Function f = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}

	/**
	 * Temporary storage for creating chains.
	 */
	private static class ChainCreationStorager {
		final Set<Integer> allBuiltNodes;
		final ClassLoader classLoader;

		// ------------------------------------------------------------------------
		//  Temporary storage for creating one chain
		// ------------------------------------------------------------------------

		final Map<Integer, StreamConfig> chainedConfigMap;
		final List<Integer> chainedHeadNodeIdsInOrder;
		final List<StreamEdge> chainInEdgesInOrder;
		final List<StreamEdge> chainOutEdgesInOrder;

		final Map<OperatorID, InputFormat> chainInputFormatMap;
		final Map<OperatorID, OutputFormat> chainOutputFormatMap;

		final Map<Integer, String> chainedNameMap;
		ResourceSpec chainedMinResources;
		ResourceSpec chainedPreferredResources;

		JobVertex createdVertex;

		final StreamTaskConfigCache vertexConfigCache;
		final List<Integer> chainedNodeIdsInOrder;

		ChainCreationStorager() {
			allBuiltNodes = new HashSet<>();
			this.classLoader = Thread.currentThread().getContextClassLoader();

			this.chainedConfigMap = new HashMap<>();
			this.chainedHeadNodeIdsInOrder = new ArrayList<>();
			this.chainInEdgesInOrder = new ArrayList<>();
			this.chainOutEdgesInOrder = new ArrayList<>();

			this.chainInputFormatMap = new HashMap<>();
			this.chainOutputFormatMap = new HashMap<>();

			this.chainedNameMap = new HashMap<>();

			this.vertexConfigCache = new StreamTaskConfigCache(classLoader);
			this.chainedNodeIdsInOrder = new ArrayList<>();
		}

		void clearChain() {
			this.chainedConfigMap.clear();
			this.chainedHeadNodeIdsInOrder.clear();
			this.chainInEdgesInOrder.clear();
			this.chainOutEdgesInOrder.clear();

			this.chainInputFormatMap.clear();
			this.chainOutputFormatMap.clear();

			this.chainedNameMap.clear();
			this.chainedMinResources = null;
			this.chainedPreferredResources = null;

			this.createdVertex = null;

			this.vertexConfigCache.clear();
			this.chainedNodeIdsInOrder.clear();
		}
	}

	/**
	 * Wrapper for a {@link StreamNode} that set chaining.
	 */
	private static class ChainingStreamNode {

		private final Integer nodeId;
		private final int depthFirstNumber;
		private final int inEdgeCount;

		private int layer = -1;
		private int breadthFirstNumber = -1;

		private Set<Integer> chainableToSet;

		private Boolean allowMultiHeadChaining;

		ChainingStreamNode(Integer nodeId, int depthFirstNumber, int inEdgeCount) {
			this.nodeId = nodeId;
			this.depthFirstNumber = depthFirstNumber;
			this.inEdgeCount = inEdgeCount;
		}

		int getNodeId() {
			return nodeId;
		}

		int getDepthFirstNumber() {
			return depthFirstNumber;
		}

		int getBreadthFirstNumber() {
			return breadthFirstNumber;
		}

		void setBreadthFirstNumber(int breadthFirstNumber) {
			this.breadthFirstNumber = breadthFirstNumber;
		}

		int getLayer() {
			return layer;
		}

		void updateLayer(int layer) {
			this.layer = Math.max(this.layer, layer);
		}

		boolean isChainHeadNode() {
			return inEdgeCount == 0 || inEdgeCount > (chainableToSet == null ? 0 : chainableToSet.size());
		}

		boolean isChainTo(Integer upstreamNodeId) {
			return chainableToSet != null && chainableToSet.contains(upstreamNodeId);
		}

		void setAllowMultiHeadChaining(Boolean allowMultiHeadChaining) {
			checkState(this.allowMultiHeadChaining == null || this.allowMultiHeadChaining == allowMultiHeadChaining,
				"The flag allowMultiHeadChaining can not be changed (nodeId: %s).", nodeId);

			this.allowMultiHeadChaining = allowMultiHeadChaining;
		}

		void chainTo(ChainingStreamNode upstreamChainingNode,
			StreamEdge edge,
			StreamNode sourceNode,
			StreamNode targetNode,
			boolean isMultiHeadChainMode,
			boolean isEagerChainingEnabled) {

			final boolean isChainable;
			if (upstreamChainingNode.allowMultiHeadChaining && isMultiHeadChainMode) {
				isChainable = isChainableOnMultiHeadMode(edge, sourceNode, targetNode, isEagerChainingEnabled);
			} else {
				isChainable = isChainable(edge, sourceNode, targetNode, isEagerChainingEnabled);
			}

			if (isChainable) {
				addChainableToNode(upstreamChainingNode.nodeId);

				setAllowMultiHeadChaining(upstreamChainingNode.allowMultiHeadChaining);
			} else {
				setAllowMultiHeadChaining(Boolean.TRUE);
			}
		}

		private void addChainableToNode(Integer upstreamNodeId) {
			if (chainableToSet == null) {
				chainableToSet = new HashSet<>();
			}
			chainableToSet.add(upstreamNodeId);
		}

		void removeChainableToNode(Integer upstreamNodeId) {
			if (chainableToSet != null) {
				chainableToSet.remove(upstreamNodeId);
			}
		}

		private boolean isChainable(StreamEdge edge,
			StreamNode upstreamNode,
			StreamNode downStreamNode,
			boolean chainEagerlyEnabled) {

			return downStreamNode.getInEdges().size() == 1
				&& isChainableOnMultiHeadMode(edge, upstreamNode, downStreamNode, chainEagerlyEnabled);
		}

		private boolean isChainableOnMultiHeadMode(StreamEdge edge,
			StreamNode upstreamNode,
			StreamNode downStreamNode,
			boolean chainEagerlyEnabled) {

			StreamOperator<?> downstreamOperator = downStreamNode.getOperator();
			StreamOperator<?> upstreamOperator = upstreamNode.getOperator();

			return downstreamOperator != null
				&& upstreamOperator != null
				&& downStreamNode.isSameSlotSharingGroup(upstreamNode)
				&& downstreamOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (upstreamOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
				upstreamOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner ||
					(downStreamNode.getParallelism() == 1 && chainEagerlyEnabled))
				&& downStreamNode.getParallelism() == upstreamNode.getParallelism()
				&& edge.getDataExchangeMode() != DataExchangeMode.BATCH;
		}
	}

	/**
	 * Generates the sequence of numbers from zero.
	 */
	private static class SequenceGenerator {

		private int sequence = 0;

		public int get() {
			return sequence++;
		}

		public int last() {
			return sequence;
		}
	}
}
