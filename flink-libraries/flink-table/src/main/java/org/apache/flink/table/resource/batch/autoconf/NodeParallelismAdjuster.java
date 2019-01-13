/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.resource.batch.autoconf;

import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.exec.ExecNode;
import org.apache.flink.table.resource.batch.NodeRunningUnit;
import org.apache.flink.table.resource.batch.ShuffleStage;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Adjust parallelism according to total cpu limit.
 */
public class NodeParallelismAdjuster {

	private final double totalCpu;
	private Map<ShuffleStage, Set<NodeRunningUnit>> overlapRunningUnits = new LinkedHashMap<>();
	private Map<NodeRunningUnit, Set<ShuffleStage>> overlapShuffleStages = new LinkedHashMap<>();

	public static void adjustParallelism(double totalCpu,
			Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap,
			Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap) {
		new NodeParallelismAdjuster(totalCpu).adjust(nodeRunningUnitMap, nodeShuffleStageMap);
	}

	private NodeParallelismAdjuster(double totalCpu) {
		this.totalCpu = totalCpu;
	}

	private void adjust(Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap, Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap) {
		buildOverlap(nodeRunningUnitMap, nodeShuffleStageMap);

		for (ShuffleStage shuffleStage : nodeShuffleStageMap.values()) {
			if (shuffleStage.isParallelismFinal()) {
				continue;
			}

			int parallelism = shuffleStage.getResultParallelism();
			for (NodeRunningUnit runningUnit : overlapRunningUnits.get(shuffleStage)) {
				int result = calculateParallelism(overlapShuffleStages.get(runningUnit), shuffleStage.getResultParallelism());
				if (result < parallelism) {
					parallelism = result;
				}
			}
			shuffleStage.setResultParallelism(parallelism, true);
		}
	}

	private void buildOverlap(Map<BatchExecNode<?>, Set<NodeRunningUnit>> nodeRunningUnitMap, Map<ExecNode<?, ?>, ShuffleStage> nodeShuffleStageMap) {
		for (ShuffleStage shuffleStage : nodeShuffleStageMap.values()) {
			Set<NodeRunningUnit> runningUnitSet = new LinkedHashSet<>();
			for (ExecNode<?, ?> node : shuffleStage.getExecNodeSet()) {
				runningUnitSet.addAll(nodeRunningUnitMap.get(node));
			}
			overlapRunningUnits.put(shuffleStage, runningUnitSet);
		}

		for (Set<NodeRunningUnit> runningUnitSet : nodeRunningUnitMap.values()) {
			for (NodeRunningUnit runningUnit : runningUnitSet) {
				if (overlapShuffleStages.containsKey(runningUnit)) {
					continue;
				}
				Set<ShuffleStage> shuffleStageSet = new LinkedHashSet<>();
				for (ExecNode<?, ?> node : runningUnit.getNodeSet()) {
					shuffleStageSet.add(nodeShuffleStageMap.get(node));
				}
				overlapShuffleStages.put(runningUnit, shuffleStageSet);
			}
		}
	}

	private int calculateParallelism(Set<ShuffleStage> shuffleStages, int parallelism) {
		double remain = totalCpu;
		double need = 0d;
		for (ShuffleStage shuffleStage : shuffleStages) {
			if (shuffleStage.isParallelismFinal()) {
				remain -= getCpu(shuffleStage, shuffleStage.getResultParallelism());
			} else {
				remain -= getCpu(shuffleStage, 1);
				need += getCpu(shuffleStage, shuffleStage.getResultParallelism() - 1);
			}
		}
		if (remain < 0) {
			throw new IllegalArgumentException("adjust parallelism error, fixed resource > remain resource.");
		}
		if (remain > need) {
			return parallelism;
		} else {
			double ratio = remain / need;
			return (int) ((parallelism - 1) * ratio) + 1;
		}
	}

	private double getCpu(ShuffleStage shuffleStage, int parallelism) {
		double totalCpu = 0;
		for (ExecNode<?, ?> node : shuffleStage.getExecNodeSet()) {
			totalCpu = Math.max(totalCpu, node.getResource().getCpu());
		}
		totalCpu *= parallelism;
		return totalCpu;
	}
}
