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

import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class OperatorChainCalculator {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorChainCalculator.class);

	private StreamGraphProperty streamGraphProperty;

	private boolean ignoreParallelism;

	public OperatorChainCalculator(
			StreamGraphProperty properties, boolean ignoreParal) {
		this.streamGraphProperty = properties;
		this.ignoreParallelism = ignoreParal;
	}

	// f[i] is ith vertex's parent
	private int[] parent;

	/**
	 * Find x's parent.
	 *
	 * @param x
	 * @return
	 */
	private int findParent(int x) {
		// found it
		if (x == parent[x]) {
			return x;
		} else {
			// path compression
			parent[x] = findParent(parent[x]);
			return parent[x];
		}
	}

	private void merge(int x, int y) {
		int fx = findParent(x);
		int fy = findParent(y);
		if (fx != fy) {
			parent[y] = fx;
		}
	}

	private void initUnionFind() {
		int maxIndex = Integer.MIN_VALUE;
		for (Map.Entry<Integer, List<Integer>> entry : streamGraphProperty.getSourceTargetMap().entrySet()) {
			maxIndex = Math.max(maxIndex, entry.getKey());
			for (Integer targetId : entry.getValue()) {
				maxIndex = Math.max(maxIndex, targetId);
			}
		}
		if (maxIndex == Integer.MIN_VALUE) {
			throw new RuntimeException("the graph has no vertex");
		}
		parent = new int[maxIndex + 1];
		for (int i = 0; i <= maxIndex; i++) {
			parent[i] = i;
		}
	}

	private void init() {
		initUnionFind();
	}

	/**
	 * If A -> B, and isChainable(A, B),
	 * then merge A, B to the same Group.
	 * This satisfies the Transitivity, use Union-Find Algorithm
	 *
	 * @return Chained groups
	 */
	public List<Collection<Integer>> getChainedGroups() {
		init();
		streamGraphProperty.getSourceTargetMap().entrySet().stream().forEach(
			entry -> entry.getValue().stream().forEach(
				targetId -> {
					if (isChainable(streamGraphProperty, entry.getKey(), targetId, ignoreParallelism)) {
						merge(entry.getKey(), targetId);
					}
				}
			)
		);
		return calcChainedGroups();
	}

	private List<Collection<Integer>> calcChainedGroups() {
		Set<Integer> idSet = new HashSet<>();
		streamGraphProperty.getSourceTargetMap().entrySet().stream().forEach(
			entry -> {
				idSet.add(entry.getKey());
				entry.getValue().stream().forEach(
					targetId -> idSet.add(targetId)
				);
			}
		);
		Map<Integer, List<Integer>> ans = new HashMap<>();
		for (int id = 0; id < parent.length; id++) {
			if (idSet.contains(id)) {
				int fid = findParent(id);
				List<Integer> list = ans.get(fid);
				if (list == null) {
					list = new ArrayList<>();
					ans.put(fid, list);
				}
				list.add(id);
			}
		}
		List<Collection<Integer>> res = ans.values().stream().collect(Collectors.toList());

		// for pretty view
		Collections.sort(res, Comparator.comparing(o -> ((List<Integer>) o).get(0)));
		return res;
	}

	/**
	 * Whether two nodes is chainable.
	 *
	 * @param resoureFile Resourcd file
	 * @param sourceId    Source id
	 * @param targetId    Target id
	 * @param ignoreParal Ignore parallelism
	 * @return True when two nodes can chain, otherwise false
	 */
	private boolean isChainable(
		StreamGraphProperty resoureFile, Integer sourceId, Integer targetId, boolean ignoreParal) {

		LOG.debug("isChainable: sourceId=" + sourceId + ", targetId=" + targetId);

		List<Integer> sourceList = resoureFile.getTargetSourceMap().get(targetId);
		if (sourceList == null || sourceList.size() != 1) {
			LOG.debug(" sourceNodes != 1");
			return false;
		}

		if (!resoureFile.isSameSlotSharingGroup(sourceId, targetId)) {
			LOG.debug(" not same slot sharing group");
			return false;
		}

		if (resoureFile.getIdToNodeMap().get(targetId).getChainingStrategy() != ChainingStrategy.ALWAYS) {
			LOG.debug(" target chainingStrategy not ALWAYS");
			return false;
		}

		switch (resoureFile.getIdToNodeMap().get(sourceId).getChainingStrategy()) {
			case ALWAYS:
			case HEAD:
				break;
			default:
				LOG.debug(" source node not HEAD and not ALWAYS ");
				return false;
		}

		if (resoureFile.getIdToEdgeMap().get(sourceId).get(targetId).size() != 1 ||
				!resoureFile.isForwardPartitioner(sourceId, targetId, 0)) {
			LOG.debug(" is not ForwardPartitioner");
			return false;
		}

		if (!ignoreParal) {
			int sourceParall = resoureFile.getIdToNodeMap().get(sourceId).getParallelism();
			int targetParall = resoureFile.getIdToNodeMap().get(targetId).getParallelism();
			if (sourceParall != targetParall) {
				LOG.debug(" parallel: " + sourceParall + " !=" + targetParall);
				return false;
			}
		}

		return true;
	}
}
