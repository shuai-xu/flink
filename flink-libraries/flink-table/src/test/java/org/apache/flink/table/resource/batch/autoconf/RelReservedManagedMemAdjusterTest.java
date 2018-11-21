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

import org.apache.flink.table.plan.nodes.physical.batch.BatchExecRel;
import org.apache.flink.table.resource.RelResource;
import org.apache.flink.table.resource.batch.RelRunningUnit;

import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for adjusting reserved managed memory.
 */
public class RelReservedManagedMemAdjusterTest {

	private List<BatchExecRel<?>> relList;
	private List<RelRunningUnit> runningUnitList;
	private Map<BatchExecRel<?>, RelResource> relResourceMap = new HashMap<>();
	private Map<BatchExecRel<?>, Integer> relParallelismMap = new HashMap<>();
	private Map<BatchExecRel<?>, Set<RelRunningUnit>> relRunningUnitMap = new LinkedHashMap<>();

	@Test
	public void testAdjust() {
		long totalMem = 100000;
		int minManagedMemory = 30;

		createRelList(5);
		createRunningUnitList(3);

		setRelRunningUnit(0, 0);
		setRelRunningUnit(1, 0, 1);
		setRelRunningUnit(2, 1, 2);
		setRelRunningUnit(3, 1);
		setRelRunningUnit(4, 2);

		setRunningUnitRelSet(0, 0, 1);
		setRunningUnitRelSet(1, 1, 2, 3);
		setRunningUnitRelSet(2, 2, 4);

		setRelResource(0, 30, 0, false);
		setRelResource(1, 30, 9000, false);
		setRelResource(2, 30, 100, false);
		setRelResource(3, 30, 200, false);
		setRelResource(4, 30, 150, true);

		setRelParallelism(0, 400);
		setRelParallelism(1, 400);
		setRelParallelism(2, 400);
		setRelParallelism(3, 400);
		setRelParallelism(4, 400);

		RelReservedManagedMemAdjuster.adjust(totalMem, relResourceMap, relParallelismMap, minManagedMemory, relRunningUnitMap);

		assertEquals(0, relResourceMap.get(relList.get(0)).getReservedManagedMem());
		assertEquals(98, relResourceMap.get(relList.get(1)).getReservedManagedMem());
		assertEquals(30, relResourceMap.get(relList.get(2)).getReservedManagedMem());
		assertEquals(32, relResourceMap.get(relList.get(3)).getReservedManagedMem());
		assertEquals(150, relResourceMap.get(relList.get(4)).getReservedManagedMem());
	}

	@Test
	public void testRemainZero() {
		long totalMem = 140;
		int minManagedMemory = 20;

		createRelList(2);
		createRunningUnitList(1);

		setRelRunningUnit(0, 0);
		setRelRunningUnit(1, 0);

		setRunningUnitRelSet(0, 0, 1);

		setRelResource(0, 10, 30, true);
		setRelResource(1, 10, 20, false);

		setRelParallelism(0, 2);
		setRelParallelism(1, 2);

		RelReservedManagedMemAdjuster.adjust(totalMem, relResourceMap, relParallelismMap, minManagedMemory, relRunningUnitMap);

		assertEquals(30, relResourceMap.get(relList.get(0)).getReservedManagedMem());
		assertEquals(20, relResourceMap.get(relList.get(1)).getReservedManagedMem());
	}

	private void setRelRunningUnit(int relIndex, int... runningUnitIndexes) {
		Set<RelRunningUnit> runningUnitSet = new LinkedHashSet<>();
		for (int runningUnitIndex : runningUnitIndexes) {
			runningUnitSet.add(runningUnitList.get(runningUnitIndex));
		}
		relRunningUnitMap.put(relList.get(relIndex), runningUnitSet);
	}

	private void setRunningUnitRelSet(int runningUnitIndex, int... relIndexes) {
		RelRunningUnit runningUnit = runningUnitList.get(runningUnitIndex);
		Set<BatchExecRel<?>> relSet = new LinkedHashSet<>();
		for (int relIndex : relIndexes) {
			relSet.add(relList.get(relIndex));
		}
		when(runningUnit.getRelSet()).thenReturn(relSet);
	}

	private void createRelList(int num) {
		relList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			relList.add(mock(BatchExecRel.class));
		}
	}

	private void setRelParallelism(int index, int parallelism) {
		relParallelismMap.put(relList.get(index), parallelism);
	}

	private void setRelResource(int index, int heap, int reservedManaged, boolean fixed) {
		RelResource resource = new RelResource();
		resource.setHeapMem(heap);
		resource.setManagedMem(reservedManaged, reservedManaged, reservedManaged, fixed);
		relResourceMap.put(relList.get(index), resource);
	}

	private void createRunningUnitList(int num) {
		runningUnitList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			runningUnitList.add(mock(RelRunningUnit.class));
		}
	}
}
