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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Adjust reserved managed mem according to total managed mem.
 */
public class RelReservedManagedMemAdjuster {

	private static final Logger LOG = LoggerFactory.getLogger(RelReservedManagedMemAdjuster.class);

	// total mem limit.
	private final long totalMem;
	// to get resource of rel.
	private final Map<RowBatchExecRel, RelResource> relResourceMap;
	// to get parallelism of rel.
	private final Map<RowBatchExecRel, Integer> relParallelismMap;

	private final int minManagedMemory;

	private RelReservedManagedMemAdjuster(long totalMem,
			Map<RowBatchExecRel, RelResource> relResourceMap,
			Map<RowBatchExecRel, Integer> relParallelismMap,
			int minManagedMemory) {
		this.totalMem = totalMem;
		this.relResourceMap = relResourceMap;
		this.relParallelismMap = relParallelismMap;
		this.minManagedMemory = minManagedMemory;
	}

	public static void adjust(long totalMem,
			Map<RowBatchExecRel, RelResource> relResourceMap,
			Map<RowBatchExecRel, Integer> relParallelismMap,
			int minManagedMemory,
			Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap) {
		new RelReservedManagedMemAdjuster(totalMem, relResourceMap, relParallelismMap, minManagedMemory).adjust(relRunningUnitMap);
	}

	private void adjust(Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap) {

		for (Map.Entry<RowBatchExecRel, Set<RelRunningUnit>> entry : relRunningUnitMap.entrySet()) {
			RowBatchExecRel rel = entry.getKey();
			RelResource resource = relResourceMap.get(rel);
			if (resource.getReservedManagedMem() == 0 || resource.isReservedManagedFinal()) {
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
			resource.setManagedMem(managed, resource.getPreferManagedMem(), resource.getMaxManagedMem(), true);
		}
	}

	private int calculateReservedManaged(RowBatchExecRel rel, RelRunningUnit runningUnit) {
		long remain = totalMem;
		long need = 0;
		LOG.debug("before calculateReserved for a runningUnit：" + runningUnit.hashCode() + ", total: " + remain + ", rel: " + rel.hashCode());
		for (RowBatchExecRel r : runningUnit.getRelSet()) {
			RelResource resource = relResourceMap.get(r);
			int parallelism = relParallelismMap.get(r);
			// minus heap mem of rel.
			remain -= (long) resource.getHeapMem() * parallelism;
			if (resource.getReservedManagedMem() == 0) {
				continue;
			}
			if (resource.isReservedManagedFinal()) {
				remain -= (long) resource.getReservedManagedMem() * parallelism;
				LOG.debug(r + ", " + r.hashCode() + " , fixed memory: " + resource.getReservedManagedMem() + ", parallelism: " + parallelism + ", remain: " + remain);
			} else {
				remain -= (long) minManagedMemory * parallelism;
				LOG.debug(r + ", " + r.hashCode() + " , reserved memory: " + resource.getReservedManagedMem() +
						", min memory:"  + minManagedMemory + ", parallelism: " + parallelism + ", remain: " + remain);
				need += (long) (resource.getReservedManagedMem() - minManagedMemory) * parallelism;
			}
		}
		if (remain < 0) {
			throw new IllegalArgumentException("total resource can not satisfy the runningUnit fixed managedMemory.");
		}
		LOG.debug("after calculateReserved for a runningUnit");
		if (remain >= need) {
			return relResourceMap.get(rel).getReservedManagedMem();
		} else {
			double ratio = (double) remain / need;
			return (int) ((relResourceMap.get(rel).getReservedManagedMem() - minManagedMemory) * ratio)
					+ minManagedMemory;
		}
	}
}
