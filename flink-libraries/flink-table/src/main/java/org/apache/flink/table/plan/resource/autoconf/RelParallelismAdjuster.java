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
import org.apache.flink.table.plan.resource.ShuffleStageInRunningUnit;

import java.util.List;
import java.util.Map;

/**
 * Adjust parallelism according to total cpu limit.
 */
public class RelParallelismAdjuster {

	private final double totalCpu;
	private final Map<RowBatchExecRel, RelResource> relResourceMap;

	public RelParallelismAdjuster(double totalCpu, Map<RowBatchExecRel, RelResource> relResourceMap) {
		this.totalCpu = totalCpu;
		this.relResourceMap = relResourceMap;
	}

	public void adjust(Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap) {
		for (ShuffleStage shuffleStage : relShuffleStageMap.values()) {
			if (shuffleStage.isParallelismFixed()) {
				continue;
			}

			int parallelism = shuffleStage.getResultParallelism();
			for (ShuffleStageInRunningUnit shuffleStageInRU : shuffleStage.getShuffleStageInRUSet()) {
				int result = calculateParallelism(shuffleStageInRU);
				if (result < parallelism) {
					parallelism = result;
				}
			}
			shuffleStage.setResultParallelism(parallelism, true);
		}
	}

	private int calculateParallelism(ShuffleStageInRunningUnit shuffleStageInRU) {
		RelRunningUnit runningUnit = shuffleStageInRU.getRelRunningUnit();
		List<ShuffleStageInRunningUnit> shuffleStageInRUs = runningUnit.getShuffleStagesInRunningUnit();
		double remain = totalCpu;
		double need = 0d;
		for (ShuffleStageInRunningUnit ss : shuffleStageInRUs) {
			if (ss.getShuffleStage().isParallelismFixed()) {
				remain -= getCpu(ss, ss.getShuffleStage().getResultParallelism());
			} else {
				remain -= getCpu(ss, 1);
				need += getCpu(ss, ss.getShuffleStage().getResultParallelism() - 1);
			}
		}
		if (remain < 0) {
			throw new IllegalArgumentException("adjust parallelism error, fixed resource > remain resource.");
		}
		if (remain > need) {
			return shuffleStageInRU.getShuffleStage().getResultParallelism();
		} else {
			double ratio = remain / need;
			return (int) ((shuffleStageInRU.getShuffleStage().getResultParallelism() - 1) * ratio) + 1;
		}
	}

	private double getCpu(ShuffleStageInRunningUnit shuffleStageInRU, int parallelism) {
		double totalCpu = 0;
		for (RowBatchExecRel rel : shuffleStageInRU.getRelSet()) {
			totalCpu = Math.max(totalCpu, relResourceMap.get(rel).getCpu());
		}
		totalCpu *= parallelism;
		return totalCpu;
	}
}
