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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Adjust reserved managed mem according to total managed mem.
 */
public class RelReservedManagedMemAdjuster {

	// total mem limit.
	private final long totalMem;
	// to get resource of rel.
	private final Map<RowBatchExecRel, RelResource> relResourceMap;
	// to get parallelism of rel.
	private final Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap;

	public RelReservedManagedMemAdjuster(long totalMem,
			Map<RowBatchExecRel, RelResource> relResourceMap,
			Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap) {
		this.totalMem = totalMem;
		this.relResourceMap = relResourceMap;
		this.relShuffleStageMap = relShuffleStageMap;
	}

	public void adjust(Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap) {
		for (Map.Entry<RowBatchExecRel, Set<RelRunningUnit>> entry : relRunningUnitMap.entrySet()) {
			RowBatchExecRel rel = entry.getKey();
			RelResource resource = relResourceMap.get(rel);
			if (resource.getReservedManagedMem() == 0 || resource.isFixedManagedMem()) {
				continue;
			}
			int managed = resource.getReservedManagedMem();
			Set<RelRunningUnit> runningUnitSet = entry.getValue();
			for (RelRunningUnit runningUnit : runningUnitSet) {
				int result = calculateReservedManaged(rel, runningUnit);
				if (result < managed) {
					managed = result;
				}
			}
			if (managed == 0) {
				throw new IllegalArgumentException("managed memory from positive to zero, total managed mem may be too little.");
			}
			resource.setManagedMem(managed, resource.getPreferManagedMem(), true);
		}
	}

	private int calculateReservedManaged(RowBatchExecRel rel, RelRunningUnit runningUnit) {
		long remain = totalMem;
		long need = 0;
		Set<RowBatchExecRel> relSet = new HashSet<>();
		for (ShuffleStageInRunningUnit ss : runningUnit.getShuffleStagesInRunningUnit()) {
			relSet.addAll(ss.getShuffleStage().getBatchExecRelSet());
		}
		for (RowBatchExecRel r : relSet) {
			RelResource resource = relResourceMap.get(r);
			int parallelism = relShuffleStageMap.get(r).getResultParallelism();
			// minus heap mem of rel.
			remain -= (long) resource.getHeapMem() * parallelism;
			if (resource.getReservedManagedMem() == 0) {
				continue;
			}
			if (resource.isFixedManagedMem()) {
				remain -= (long) resource.getReservedManagedMem() * parallelism;
			} else {
				need += (long) resource.getReservedManagedMem() * parallelism;
			}
		}
		if (remain <= 0) {
			throw new IllegalArgumentException("total resource can not satisfy the runningUnit fixed managedMemory.");
		}
		if (remain >= need) {
			return relResourceMap.get(rel).getReservedManagedMem();
		} else {
			double ratio = (double) remain / need;
			return (int) (relResourceMap.get(rel).getReservedManagedMem() * ratio);
		}
	}
}
