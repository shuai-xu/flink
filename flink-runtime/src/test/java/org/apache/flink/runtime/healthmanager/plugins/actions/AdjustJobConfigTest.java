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

package org.apache.flink.runtime.healthmanager.plugins.actions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for AdjustJobConfigTest.
 */
public class AdjustJobConfigTest {

	@Test
	public void testExculdeMinorDiffVertices() {
		AdjustJobConfig adjustJobConfig = new AdjustJobConfig(new JobID(), 0L);

		JobVertexID vertex1 = new JobVertexID();
		adjustJobConfig.addVertex(vertex1, 100, 105,
			ResourceSpec.newBuilder().build(),
			ResourceSpec.newBuilder().build());

		JobVertexID vertex2 = new JobVertexID();
		adjustJobConfig.addVertex(vertex2, 1, 1,
			ResourceSpec.newBuilder().setCpuCores(0.1).build(),
			ResourceSpec.newBuilder().setCpuCores(0.15).build());

		JobVertexID vertex3 = new JobVertexID();
		adjustJobConfig.addVertex(vertex3, 1, 1,
			ResourceSpec.newBuilder().setHeapMemoryInMB(100).build(),
			ResourceSpec.newBuilder().setHeapMemoryInMB(150).build());

		JobVertexID vertex4 = new JobVertexID();
		adjustJobConfig.addVertex(vertex4, 1, 1,
			ResourceSpec.newBuilder().setDirectMemoryInMB(2000).build(),
			ResourceSpec.newBuilder().setDirectMemoryInMB(2150).build());

		JobVertexID vertex5 = new JobVertexID();
		adjustJobConfig.addVertex(vertex5, 1, 1,
			ResourceSpec.newBuilder().setNativeMemoryInMB(100).build(),
			ResourceSpec.newBuilder().setNativeMemoryInMB(200).build());

		adjustJobConfig.exculdeMinorDiffVertices(new Configuration());

		assertEquals(1, adjustJobConfig.currentParallelism.size());
		assertTrue(adjustJobConfig.currentParallelism.containsKey(vertex5));
	}
}
