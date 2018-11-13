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

package org.apache.flink.table.resource.batch.calculator;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecExchange;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecScan;
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecUnion;
import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.MockRelTestBase;
import org.apache.flink.table.resource.RelResource;

import org.apache.calcite.rel.RelDistribution;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for BatchRelCpuHeapMemCalculator.
 */
public class BatchRelCpuHeapMemCalculatorTest extends MockRelTestBase {
	private StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
	private BatchTableEnvironment tEnv;
	private Map<RowBatchExecRel, RelResource> relResMap;
	private BatchRelCpuHeapMemCalculator relCpuHeapMemCalculator;

	@Before
	public void setUp() {
		tEnv = TableEnvironment.getBatchTableEnvironment(sEnv);
		relResMap = new HashMap<>();
		this.relCpuHeapMemCalculator = new BatchRelCpuHeapMemCalculator(tEnv);
		this.relCpuHeapMemCalculator.setRelResourceMap(relResMap);
	}

	@Test
	public void testCalc() {
		/**
		 *           0, Source    1, Source
		 *                 \    /
		 *                2, Union
		 *                  /    \
		 *            3, Calc     \
		 *               \         \
		 *            4, Exchange  5, Exchange
		 *                \         /
		 *                 6, HashJoin
		 */
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_SOURCE_MEM(), 40);
		tEnv.getConfig().getParameters().setInteger(TableConfig.SQL_EXEC_DEFAULT_MEM(), 20);
		tEnv.getConfig().getParameters().setDouble(TableConfig.SQL_EXEC_DEFAULT_CPU(), 0.5);
		createRelList(7);
		BatchExecScan scan0 = mock(BatchExecScan.class);
		updateRel(0, scan0);
		BatchExecScan scan1 = mock(BatchExecScan.class);
		updateRel(1, scan1);
		when(scan1.getTableSourceResource(tEnv)).thenReturn(buildResourceSpec(0.7d, 50));
		when(scan1.needInternalConversion()).thenReturn(true);
		updateRel(2, mock(BatchExecUnion.class));
		BatchExecExchange execExchange5 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange5.getDistribution().getType()).thenReturn(RelDistribution.Type.RANGE_DISTRIBUTED);
		updateRel(4, execExchange5);
		BatchExecExchange execExchange6 = mock(BatchExecExchange.class, RETURNS_DEEP_STUBS);
		when(execExchange6.getDistribution().getType()).thenReturn(RelDistribution.Type.BROADCAST_DISTRIBUTED);
		updateRel(5, execExchange6);
		connect(2, 0, 1);
		connect(3, 2);
		connect(4, 3);
		connect(5, 2);
		connect(6, 4, 5);
		relCpuHeapMemCalculator.calculate(relList.get(6));
		assertEquals(buildResource(0.5d, 40), relResMap.get(relList.get(0)));
		assertEquals(buildResource(0.7d, 70), relResMap.get(relList.get(1)));
		assertEquals(buildResource(0.5d, 20), relResMap.get(relList.get(3)));
		assertEquals(buildResource(0.5d, 20), relResMap.get(relList.get(6)));
		assertEquals(4, relResMap.size());
		verify(relList.get(4)).setResource(buildResource(0.5, 20));
		verify(relList.get(2), never()).setResource(any());
		verify(relList.get(5), never()).setResource(any());
	}

	private RelResource buildResource(double cpu, int heap) {
		RelResource resource = new RelResource();
		resource.setCpu(cpu);
		resource.setHeapMem(heap);
		return resource;
	}

	private ResourceSpec buildResourceSpec(double cpu, int heap) {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heap);
		return builder.build();
	}
}
