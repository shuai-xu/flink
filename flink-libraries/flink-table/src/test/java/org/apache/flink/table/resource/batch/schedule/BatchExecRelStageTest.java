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

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.table.plan.nodes.physical.batch.RowBatchExecRel;
import org.apache.flink.table.resource.batch.BatchExecRelStage;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test for BatchExecRelStage.
 */
public class BatchExecRelStageTest {

	@Test
	public void testDependStages() {
		RowBatchExecRel rel = mock(RowBatchExecRel.class);
		BatchExecRelStage relStage = new BatchExecRelStage(rel, 0);
		BatchExecRelStage relStage1 = new BatchExecRelStage(rel, 1);
		relStage.addDependStage(relStage1, BatchExecRelStage.DependType.DATA_TRIGGER);
		assertEquals(1, relStage.getAllDependStageList().size());
		BatchExecRelStage relStage2 = new BatchExecRelStage(rel, 2);
		relStage.addDependStage(relStage2, BatchExecRelStage.DependType.PRIORITY);
		assertEquals(2, relStage.getAllDependStageList().size());
		BatchExecRelStage relStage3 = new BatchExecRelStage(rel, 3);
		relStage.addDependStage(relStage3, BatchExecRelStage.DependType.DATA_TRIGGER);
		assertEquals(3, relStage.getAllDependStageList().size());
		relStage.removeDependStage(relStage2);
		assertEquals(2, relStage.getAllDependStageList().size());
	}
}
