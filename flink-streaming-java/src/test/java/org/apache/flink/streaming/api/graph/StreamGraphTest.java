/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@code StreamGraph}.
 */
public class StreamGraphTest {

	@Test
	public void testSettingResultPartitionType() {
		StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
			new CheckpointConfig(),
			3,
			1234L,
			ResultPartitionType.PIPELINED_BOUNDED,
			DataPartitionerType.REBALANCE);

		streamGraph.addNode(999, null, null, null, null);
		streamGraph.addNode(888, null, null, null, null);
		streamGraph.addNode(111, null, null, null, null);
		streamGraph.addNode(222, null, null, null, null);
		streamGraph.addNode(333, null, null, null, null);
		streamGraph.addNode(444, null, null, null, null);

		streamGraph.addVirtualPartitionNode(999, 87, null, null);
		streamGraph.addVirtualPartitionNode(888, 56, null, ResultPartitionType.BLOCKING);

		streamGraph.addEdge(87, 111, 0, ResultPartitionType.PIPELINED);
		streamGraph.addEdge(87, 222, 0, null);
		streamGraph.addEdge(56, 333, 0, ResultPartitionType.PIPELINED);
		streamGraph.addEdge(56, 444, 0, null);

		assertEquals(ResultPartitionType.PIPELINED, streamGraph.getStreamEdges(999, 111).get(0).getResultPartitionType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, streamGraph.getStreamEdges(999, 222).get(0).getResultPartitionType());
		assertEquals(ResultPartitionType.PIPELINED, streamGraph.getStreamEdges(888, 333).get(0).getResultPartitionType());
		assertEquals(ResultPartitionType.BLOCKING, streamGraph.getStreamEdges(888, 444).get(0).getResultPartitionType());
	}

	@Test
	public void testDataPartitionerType() {
		// case: set the default partitioner type to REBALANCE
		{
			StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
				new CheckpointConfig(),
				3,
				1234L,
				ResultPartitionType.PIPELINED_BOUNDED,
				DataPartitionerType.REBALANCE);

			streamGraph.addNode(999, null, null, null, null);
			streamGraph.addNode(888, null, null, null, null);
			streamGraph.addNode(777, null, null, null, null)
				.setParallelism(100);

			streamGraph.addEdge(999, 888, 0, null);
			streamGraph.addEdge(888, 777, 0, null);

			assertEquals(ForwardPartitioner.class, streamGraph.getStreamEdges(999, 888).get(0).getPartitioner().getClass());
			assertEquals(RebalancePartitioner.class, streamGraph.getStreamEdges(888, 777).get(0).getPartitioner().getClass());
		}

		// case: set the default partitioner type to RESCALE
		{
			StreamGraph streamGraph = new StreamGraph(new ExecutionConfig(),
				new CheckpointConfig(),
				3,
				1234L,
				ResultPartitionType.PIPELINED_BOUNDED,
				DataPartitionerType.RESCALE);

			streamGraph.addNode(999, null, null, null, null);
			streamGraph.addNode(888, null, null, null, null);
			streamGraph.addNode(777, null, null, null, null)
				.setParallelism(100);

			streamGraph.addEdge(999, 888, 0, null);
			streamGraph.addEdge(888, 777, 0, null);

			assertEquals(ForwardPartitioner.class, streamGraph.getStreamEdges(999, 888).get(0).getPartitioner().getClass());
			assertEquals(RescalePartitioner.class, streamGraph.getStreamEdges(888, 777).get(0).getPartitioner().getClass());
		}
	}
}
