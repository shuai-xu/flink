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

package org.apache.flink.table.plan.resource.autoconf;

import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.RelResource;
import org.apache.flink.table.plan.resource.RelRunningUnit;
import org.apache.flink.table.plan.resource.ShuffleStage;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Adjust parallelism according to total cpu limit.
 */
public class RelParallelismAdjuster {

	private final double totalCpu;
	private final Map<RowBatchExecRel, RelResource> relResourceMap;
	private Map<ShuffleStage, Set<RelRunningUnit>> overlapRunningUnits = new LinkedHashMap<>();
	private Map<RelRunningUnit, Set<ShuffleStage>> overlapShuffleStages = new LinkedHashMap<>();

	public static void adjustParallelism(double totalCpu,
			Map<RowBatchExecRel, RelResource> relResourceMap,
			Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap,
			Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap) {
		new RelParallelismAdjuster(totalCpu, relResourceMap).adjust(relRunningUnitMap, relShuffleStageMap);
	}

	private RelParallelismAdjuster(double totalCpu, Map<RowBatchExecRel, RelResource> relResourceMap) {
		this.totalCpu = totalCpu;
		this.relResourceMap = relResourceMap;
	}

	private void adjust(Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap, Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap) {
		buildOverlap(relRunningUnitMap, relShuffleStageMap);

		for (ShuffleStage shuffleStage : relShuffleStageMap.values()) {
			if (shuffleStage.isParallelismFinal()) {
				continue;
			}

			int parallelism = shuffleStage.getResultParallelism();
			for (RelRunningUnit runningUnit : overlapRunningUnits.get(shuffleStage)) {
				int result = calculateParallelism(overlapShuffleStages.get(runningUnit), shuffleStage.getResultParallelism());
				if (result < parallelism) {
					parallelism = result;
				}
			}
			shuffleStage.setResultParallelism(parallelism, true);
		}
	}

	private void buildOverlap(Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap, Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap) {
		for (ShuffleStage shuffleStage : relShuffleStageMap.values()) {
			Set<RelRunningUnit> runningUnitSet = new LinkedHashSet<>();
			for (RowBatchExecRel rel : shuffleStage.getBatchExecRelSet()) {
				runningUnitSet.addAll(relRunningUnitMap.get(rel));
			}
			overlapRunningUnits.put(shuffleStage, runningUnitSet);
		}

		for (Set<RelRunningUnit> runningUnitSet : relRunningUnitMap.values()) {
			for (RelRunningUnit runningUnit : runningUnitSet) {
				if (overlapShuffleStages.containsKey(runningUnit)) {
					continue;
				}
				Set<ShuffleStage> shuffleStageSet = new LinkedHashSet<>();
				for (RowBatchExecRel rel : runningUnit.getRelSet()) {
					shuffleStageSet.add(relShuffleStageMap.get(rel));
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
		for (RowBatchExecRel rel : shuffleStage.getBatchExecRelSet()) {
			totalCpu = Math.max(totalCpu, relResourceMap.get(rel).getCpu());
		}
		totalCpu *= parallelism;
		return totalCpu;
	}
}
