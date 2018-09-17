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

package org.apache.flink.table.plan.resource;

import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.plan.resource.autoconf.RelParallelismAdjuster;

import org.junit.Before;
import org.junit.Test;

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
 * Test for {@link RelParallelismAdjuster}.
 */
public class RelParallelismAdjusterTest {

	private double totalCpu = 100;
	private List<RowBatchExecRel> relList;
	private Map<RowBatchExecRel, RelResource> relResourceMap;
	private Map<RowBatchExecRel, Set<RelRunningUnit>> relRunningUnitMap;
	private Map<RowBatchExecRel, ShuffleStage> relShuffleStageMap;

	@Before
	public void setUp() {
		relResourceMap = new LinkedHashMap<>();
		relRunningUnitMap = new LinkedHashMap<>();
		relShuffleStageMap = new LinkedHashMap<>();
	}

	@Test
	public void testNotAdjustParallelism() {
		createRelList(5);
		putSameRunningUnit(0, 1, 2, 3, 4);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 4);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 5);
		putSameShuffleStage(4);
		setShuffleStageParallelism(4, 7);

		setRelCpu(0, 0.5);
		setRelCpu(1, 0.5);
		setRelCpu(2, 0.5);
		setRelCpu(3, 0.5);
		setRelCpu(4, 0.5);

		RelParallelismAdjuster.adjustParallelism(totalCpu, relResourceMap, relRunningUnitMap, relShuffleStageMap);
		assertEquals(4, relShuffleStageMap.get(relList.get(0)).getResultParallelism());
		assertEquals(4, relShuffleStageMap.get(relList.get(1)).getResultParallelism());
		assertEquals(5, relShuffleStageMap.get(relList.get(2)).getResultParallelism());
		assertEquals(5, relShuffleStageMap.get(relList.get(3)).getResultParallelism());
		assertEquals(7, relShuffleStageMap.get(relList.get(4)).getResultParallelism());
	}

	@Test
	public void testOverlap() {
		createRelList(9);
		putSameRunningUnit(0, 1, 2, 3, 4, 5);
		putSameRunningUnit(3, 6);
		putSameRunningUnit(6, 7, 8);
		putSameRunningUnit(7, 8, 5);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 200);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 6000);
		putSameShuffleStage(4, 5);
		setShuffleStageParallelism(4, 7000);
		putSameShuffleStage(6, 7);
		setShuffleStageParallelism(6, 8000);
		putSameShuffleStage(8);
		setShuffleStageParallelism(8, 4000);

		setRelCpu(0, 0.2);
		setRelCpu(1, 0.3);
		setRelCpu(2, 0.4);
		setRelCpu(3, 0.5);
		setRelCpu(4, 0.6);
		setRelCpu(5, 0.55);
		setRelCpu(6, 0.45);
		setRelCpu(7, 0.35);
		setRelCpu(8, 0.25);

		RelParallelismAdjuster.adjustParallelism(totalCpu, relResourceMap, relRunningUnitMap, relShuffleStageMap);
		assertEquals(3, relShuffleStageMap.get(relList.get(0)).getResultParallelism());
		assertEquals(3, relShuffleStageMap.get(relList.get(1)).getResultParallelism());
		assertEquals(82, relShuffleStageMap.get(relList.get(2)).getResultParallelism());
		assertEquals(82, relShuffleStageMap.get(relList.get(3)).getResultParallelism());
		assertEquals(79, relShuffleStageMap.get(relList.get(4)).getResultParallelism());
		assertEquals(79, relShuffleStageMap.get(relList.get(5)).getResultParallelism());
		assertEquals(91, relShuffleStageMap.get(relList.get(6)).getResultParallelism());
		assertEquals(91, relShuffleStageMap.get(relList.get(7)).getResultParallelism());
		assertEquals(46, relShuffleStageMap.get(relList.get(8)).getResultParallelism());
	}

	@Test
	public void testAllInARunningUnit() {
		createRelList(5);
		putSameRunningUnit(0, 1, 2, 3, 4);

		putSameShuffleStage(0, 1);
		setShuffleStageParallelism(0, 1000);
		putSameShuffleStage(2, 3);
		setShuffleStageParallelism(2, 2000);
		putSameShuffleStage(4);
		setShuffleStageParallelism(4, 5000);

		setRelCpu(0, 0.5);
		setRelCpu(1, 0.5);
		setRelCpu(2, 0.5);
		setRelCpu(3, 0.5);
		setRelCpu(4, 0.5);

		RelParallelismAdjuster.adjustParallelism(totalCpu, relResourceMap, relRunningUnitMap, relShuffleStageMap);
		assertEquals(25, relShuffleStageMap.get(relList.get(0)).getResultParallelism());
		assertEquals(25, relShuffleStageMap.get(relList.get(1)).getResultParallelism());
		assertEquals(50, relShuffleStageMap.get(relList.get(2)).getResultParallelism());
		assertEquals(50, relShuffleStageMap.get(relList.get(3)).getResultParallelism());
		assertEquals(125, relShuffleStageMap.get(relList.get(4)).getResultParallelism());
	}

	@Test
	public void testAllInAShuffleStage() {
		createRelList(5);
		putSameRunningUnit(0, 1);
		putSameRunningUnit(1, 2);
		putSameRunningUnit(2, 3, 4);

		putSameShuffleStage(0, 1, 2, 3, 4);
		setShuffleStageParallelism(0, Integer.MAX_VALUE);

		setRelCpu(0, 0.2);
		setRelCpu(1, 0.3);
		setRelCpu(2, 0.4);
		setRelCpu(3, 0.5);
		setRelCpu(4, 0.6);

		RelParallelismAdjuster.adjustParallelism(totalCpu, relResourceMap, relRunningUnitMap, relShuffleStageMap);

		assertEquals(166, relShuffleStageMap.get(relList.get(0)).getResultParallelism());
		assertEquals(166, relShuffleStageMap.get(relList.get(1)).getResultParallelism());
		assertEquals(166, relShuffleStageMap.get(relList.get(2)).getResultParallelism());
		assertEquals(166, relShuffleStageMap.get(relList.get(3)).getResultParallelism());
		assertEquals(166, relShuffleStageMap.get(relList.get(4)).getResultParallelism());
	}

	private void putSameRunningUnit(int... indexes) {
		RelRunningUnit runningUnit = new RelRunningUnit();
		for (int index : indexes) {
			RowBatchExecRel rel = relList.get(index);
			runningUnit.addRelStage(new BatchExecRelStage(rel, 0));
		}
		for (int index : indexes) {
			RowBatchExecRel rel = relList.get(index);
			relRunningUnitMap.computeIfAbsent(rel, k->new LinkedHashSet<>()).add(runningUnit);
		}
	}

	private void setShuffleStageParallelism(int relIndex, int parallelism) {
		relShuffleStageMap.get(relList.get(relIndex)).setResultParallelism(parallelism, false);
	}

	public void putSameShuffleStage(int... indexes) {
		ShuffleStage shuffleStage = new ShuffleStage();
		for (int index : indexes) {
			RowBatchExecRel rel = relList.get(index);
			shuffleStage.addRel(rel);
		}
		for (int index : indexes) {
			RowBatchExecRel rel = relList.get(index);
			relShuffleStageMap.put(rel, shuffleStage);
		}
	}

	private void setRelCpu(int index, double cpu) {
		relResourceMap.get(relList.get(index)).setCpu(cpu);
	}

	private void createRelList(int num) {
		relList = new LinkedList<>();
		for (int i = 0; i < num; i++) {
			RowBatchExecRel rel = mock(RowBatchExecRel.class);
			when(rel.toString()).thenReturn("id: " + i);
			relList.add(rel);
			RelResource resource = new RelResource();
			relResourceMap.put(relList.get(i), new RelResource());
		}
	}
}
