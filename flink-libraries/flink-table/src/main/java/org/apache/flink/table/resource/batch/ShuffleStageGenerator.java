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

package org.apache.flink.table.resource.batch;

import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;


/**
 * Build shuffleStages.
 */
public class ShuffleStageGenerator {

	private final Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap = new LinkedHashMap<>();

	public static Map<ExecNode<?, ?>, ShuffleStage> generate(ExecNode<?, ?> rootNode) {
		ShuffleStageGenerator generator = new ShuffleStageGenerator();
		generator.buildShuffleStages(rootNode);
		Map<ExecNode<?, ?>, ShuffleStage> result = generator.getNodeShuffleStageMap();
		result.values().forEach(s -> {
			List<ExecNode<?, ?>> virtualNodeList = s.getExecNodeSet().stream().filter(ShuffleStageGenerator::isVirtualNode).collect(toList());
			virtualNodeList.forEach(s::removeNode);
		});
		return generator.getNodeShuffleStageMap().entrySet().stream()
				.filter(x -> !isVirtualNode(x.getKey()))
				.collect(Collectors.toMap(Map.Entry::getKey,
						Map.Entry::getValue,
						(e1, e2) -> e1,
						LinkedHashMap::new));
	}

	private void buildShuffleStages(ExecNode<?, ?> execNode) {
		if (nodeShuffleStageMap.containsKey(execNode)) {
			return;
		}
		for (ExecNode<?, ?> input : execNode.getInputNodes()) {
			buildShuffleStages((input));
		}

		if (execNode.getInputNodes().isEmpty()) {
			// source node
			ShuffleStage shuffleStage = new ShuffleStage();
			shuffleStage.addNode(execNode);
			if (execNode.getResource().isParallelismMax()) {
				shuffleStage.setResultParallelism(execNode.getResource().getParallelism(), true);
			}
			nodeShuffleStageMap.put(execNode, shuffleStage);
		} else if (!(execNode instanceof BatchExecExchange)) {
			Set<ShuffleStage> inputShuffleStages = getInputShuffleStages(execNode);
			ShuffleStage inputShuffleStage = mergeInputShuffleStages(inputShuffleStages, execNode.getResource().getParallelism());
			inputShuffleStage.addNode(execNode);
			nodeShuffleStageMap.put(execNode, inputShuffleStage);
		}
	}

	private ShuffleStage mergeInputShuffleStages(Set<ShuffleStage> shuffleStageSet, int parallelism) {
		if (parallelism > 0) {
			ShuffleStage resultShuffleStage = new ShuffleStage();
			resultShuffleStage.setResultParallelism(parallelism, true);
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				if (!shuffleStage.isParallelismFinal() || shuffleStage.getResultParallelism() == parallelism) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		} else {
			ShuffleStage resultShuffleStage = shuffleStageSet.stream()
					.filter(ShuffleStage::isParallelismFinal)
					.max(Comparator.comparing(ShuffleStage::getResultParallelism))
					.orElse(new ShuffleStage());
			for (ShuffleStage shuffleStage : shuffleStageSet) {
				if (!shuffleStage.isParallelismFinal() || shuffleStage.getResultParallelism() == resultShuffleStage.getResultParallelism()) {
					mergeShuffleStage(resultShuffleStage, shuffleStage);
				}
			}
			return resultShuffleStage;
		}
	}

	private void mergeShuffleStage(ShuffleStage shuffleStage, ShuffleStage other) {
		Set<ExecNode<?, ?>> nodeSet = other.getExecNodeSet();
		shuffleStage.addNodeSet(nodeSet);
		for (ExecNode<?, ?> r : nodeSet) {
			nodeShuffleStageMap.put(r, shuffleStage);
		}
	}

	private Set<ShuffleStage> getInputShuffleStages(ExecNode<?, ?> node) {
		Set<ShuffleStage> shuffleStageList = new HashSet<>();
		for (ExecNode<?, ?> input : node.getInputNodes()) {
			ShuffleStage oneInputShuffleStage = nodeShuffleStageMap.get(input);
			if (oneInputShuffleStage != null) {
				shuffleStageList.add(oneInputShuffleStage);
			}
		}
		return shuffleStageList;
	}

	private static boolean isVirtualNode(ExecNode<?, ?> node) {
		return node instanceof BatchExecUnion;
	}

	private Map<ExecNode<?, ?>, ShuffleStage> getNodeShuffleStageMap() {
		return nodeShuffleStageMap;
	}
}
