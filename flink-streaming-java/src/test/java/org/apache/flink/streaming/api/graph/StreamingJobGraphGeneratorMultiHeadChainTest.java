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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.ArbitraryInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTaskV2;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigSnapshot;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGeneratorTest.verifyVertex;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StreamingJobGraphGenerator}.
 */
@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorMultiHeadChainTest extends TestLogger {

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     CHAIN(Source1  -+            +- Sink1)
	 *          (          | -> Map1 -> |       )
	 *          (Source2  -+            +- Sink2)
	 * </pre>
	 */
	@Test
	public void testSimpleTopology() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
		DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

		DataStream<String> map1 = source1.connect(source2)
			.map(new NoOpCoMapFunction()).name("map1");

		map1.addSink(new NoOpSinkFunction()).name("sink1");
		map1.addSink(new NoOpSinkFunction()).name("sink2");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(1, jobGraph.getNumberOfVertices());

		JobVertex vertex = jobGraph.getVertices().iterator().next();
		assertEquals(0, vertex.getInputs().size());
		assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

		StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
		verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
			Collections.emptyList(),
			Collections.emptyList(),
			Sets.newHashSet("Source: source1", "Source: source2", "map1", "Sink: sink1", "Sink: sink2"),
			Sets.newHashSet("Source: source1", "Source: source2"),
			taskonfig, streamGraph);
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     CHAIN(Source1) -> CHAIN(Filter1 -+                 )
	 *          (   |   )         (         | -> Map1 -> Sink1)
	 *          (   |_  ) ->      (        -+                 )
	 * </pre>
	 */
	@Test
	public void testTriangleTopology() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
		DataStream<String> filter1 = source1.rescale().filter(new NoOpFilterFunction()).name("filter1");

		DataStream<String> map1 = filter1.connect(source1)
			.map(new NoOpCoMapFunction()).name("map1");
		map1.addSink(new NoOpSinkFunction()).name("sink1");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(2, jobGraph.getNumberOfVertices());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		// CHAIN(Source1) vertex
		{
			JobVertex vertex = verticesSorted.get(0);
			assertEquals(0, vertex.getInputs().size());
			assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "map1"), Tuple2.of("Source: source1", "filter1")),
				Sets.newHashSet("Source: source1"),
				Sets.newHashSet("Source: source1"),
				taskonfig, streamGraph);
		}

		// CHAIN([Filter1 -> Map1, Map1 -> Sink1]) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());
			assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("Source: source1", "map1"), Tuple2.of("Source: source1", "filter1")),
				Collections.emptyList(),
				Sets.newHashSet("filter1", "map1", "Sink: sink1"),
				Sets.newHashSet("filter1", "map1"),
				taskonfig, streamGraph);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     CHAIN(Source1 -> Filter1) -> CHAIN(        -+                     )
	 *          (   |              )         (         | -> Process1 -> Sink1)
	 *          (   |_  -> Map1    ) ->      (Filter2 -+                     )
	 * </pre>
	 */
	@Test
	public void testRhombusTopology() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
		DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

		DataStream<String> filter2 = source1.map(new NoOpMapFunction()).name("map1").rescale()
			.filter(new NoOpFilterFunction()).name("filter2");

		DataStream<String> process1 = filter2.connect(filter1)
			.process(new NoOpCoProcessFuntion()).name("process1");
		process1.addSink(new NoOpSinkFunction()).name("sink1");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(2, jobGraph.getNumberOfVertices());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		// CHAIN(Source1 -> (Filter1, Map1)) vertex
		{
			JobVertex vertex = verticesSorted.get(0);
			assertEquals(0, vertex.getInputs().size());
			assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "process1"), Tuple2.of("map1", "filter2")),
				Sets.newHashSet("Source: source1", "filter1", "map1"),
				Sets.newHashSet("Source: source1"),
				taskonfig, streamGraph);
		}

		// CHAIN([Filter2 -> Process1, Process1 -> Sink1]) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());
			assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "process1"), Tuple2.of("map1", "filter2")),
				Collections.emptyList(),
				Sets.newHashSet("filter2", "process1", "Sink: sink1"),
				Sets.newHashSet("filter2", "process1"),
				taskonfig, streamGraph);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     CHAIN(RunnableSource1 -> Map1) -+
	 *                                     | -> CHAIN(Process1 -> Sink1)
	 *     CHAIN(RunnableSource2 -> Map2) -+         (   |_    -> Sink2)
	 * </pre>
	 */
	@Test
	public void testRunnableSourceSimpleTopology() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");

		DataStream<String> source2 = env.addSource(new NoOpSourceFunction()).name("source2");
		DataStream<String> map2 = source2.map(new NoOpMapFunction()).name("map2");

		DataStream<String> process1 = map1.connect(map2)
			.process(new NoOpCoProcessFuntion()).name("process1");

		process1.addSink(new NoOpSinkFunction()).name("sink1");
		process1.addSink(new NoOpSinkFunction()).name("sink2");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(3, jobGraph.getNumberOfVertices());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		// CHAIN(Source1 -> Map1) vertex
		{
			JobVertex vertex = verticesSorted.get(0);
			assertEquals(0, vertex.getInputs().size());
			assertEquals(SourceStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("map1", "process1")),
				Sets.newHashSet("Source: source1", "map1"),
				Sets.newHashSet("Source: source1"),
				taskonfig, streamGraph);
		}

		// CHAIN(Source2 -> Map2) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(0, vertex.getInputs().size());
			assertEquals(SourceStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("map2", "process1")),
				Sets.newHashSet("Source: source2", "map2"),
				Sets.newHashSet("Source: source2"),
				taskonfig, streamGraph);
		}

		// CHAIN(Process1 -> (Sink1, Sink2)) vertex
		{
			JobVertex vertex = verticesSorted.get(2);
			assertEquals(2, vertex.getInputs().size());
			assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("map1", "process1"), Tuple2.of("map2", "process1")),
				Collections.emptyList(),
				Sets.newHashSet("process1", "Sink: sink1", "Sink: sink2"),
				Sets.newHashSet("process1"),
				taskonfig, streamGraph);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     CHAIN(RunnableSource1 -> Filter1 -> Sink1)
	 *          (           |           |_          ) -+
	 *          (           |                       )  | -> CHAIN(Map1 -> Sink2)
	 *          (           |_                      ) -+
	 *          (           |_   -> Filter2         )
	 *                                  |_ -+
	 *     CHAIN(Source2 -+                 | -> Process1                       ) -+
	 *          (         | -> Map2        -+       |_   -+                     )  |
	 *          (Source3 -+     |                         | -> Process2 -> Sink3)  | -> CHAIN(Process3 -> Sink4)
	 *          (               |                Source4 -+                     )  |
	 *          (               |_                                              ) -+
	 * </pre>
	 */
	@Test
	public void testRunnableSourceComplexTopology() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
		DataStream<String> filter2 = source1.filter(new NoOpFilterFunction()).name("filter2");

		filter1.addSink(new NoOpSinkFunction()).name("sink1");

		filter1.connect(source1).map(new NoOpCoMapFunction()).name("map1")
			.addSink(new NoOpSinkFunction()).name("sink2");

		DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
		DataStream<String> source3 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source3");
		DataStream<String> source4 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source4");

		DataStream<String> map2 = source2.connect(source3).map(new NoOpCoMapFunction()).name("map2");

		DataStream<String> process1 = filter2.connect(map2).process(new NoOpCoProcessFuntion()).name("process1");

		process1.connect(source4).process(new NoOpCoProcessFuntion()).name("process2")
			.addSink(new NoOpSinkFunction()).name("sink3");

		process1.connect(map2).process(new NoOpCoProcessFuntion()).name("process3")
			.addSink(new NoOpSinkFunction()).name("sink4");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(4, jobGraph.getNumberOfVertices());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		// CHAIN([Source1 -> (Filter1, Filter2), Filter1 -> Sink1]) vertex
		{
			JobVertex vertex = verticesSorted.get(0);
			assertEquals(0, vertex.getInputs().size());
			assertEquals(SourceStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1"), Tuple2.of("filter2", "process1")),
				Sets.newHashSet("Source: source1", "filter1", "filter2", "Sink: sink1"),
				Sets.newHashSet("Source: source1"),
				taskonfig, streamGraph);
		}

		// CHAIN(Map1 -> Sink2) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());
			assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
				Collections.emptyList(),
				Sets.newHashSet("map1", "Sink: sink2"),
				Sets.newHashSet("map1"),
				taskonfig, streamGraph);
		}

		// CHAIN([(Source2, Source3) -> Map2, Map2 -> Process1, (Process1, Source4) -> Process2, Process2 -> Sink3]) vertex
		{
			JobVertex vertex = verticesSorted.get(2);
			assertEquals(1, vertex.getInputs().size());
			assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter2", "process1")),
				Lists.newArrayList(Tuple2.of("process1", "process3"), Tuple2.of("map2", "process3")),
				Sets.newHashSet("Source: source2", "Source: source3", "Source: source4", "map2", "process1", "process2", "Sink: sink3"),
				Sets.newHashSet("Source: source2", "Source: source3", "Source: source4", "process1"),
				taskonfig, streamGraph);
		}

		// CHAIN(Process3 -> Sink4) vertex
		{
			JobVertex vertex = verticesSorted.get(3);
			assertEquals(2, vertex.getInputs().size());
			assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("process1", "process3"), Tuple2.of("map2", "process3")),
				Collections.emptyList(),
				Sets.newHashSet("process3", "Sink: sink4"),
				Sets.newHashSet("process3"),
				taskonfig, streamGraph);
		}
	}

	/**
	 * Tests for breaking off chains.
	 */
	@RunWith(Parameterized.class)
	public static class ParameterizedBreakingOffTest {
		private final int breakingOffType;

		@Parameterized.Parameters(name = "breakingOffType={0}")
		public static Collection<Integer> getParameters() {
			return Arrays.asList(new Integer[]{
				0, 1, 2, 3
			});
		}

		public ParameterizedBreakingOffTest(int breakOffChainType) {
			this.breakingOffType = breakOffChainType;
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Filter1) -+
		 *          (   |              )  | -> CHAIN(Map1 -> Sink1)
		 *          (   |_             ) -+
		 * </pre>
		 */
		@Test
		public void testTriangleTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> map1 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? source1.rescale() : source1)
				.map(new NoOpCoMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN(Source1 -> Filter1) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Sets.newHashSet("Source: source1", "filter1"),
					Sets.newHashSet("Source: source1"),
					taskonfig, streamGraph);
			}

			// CHAIN(Map1 -> Sink1) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Collections.emptyList(),
					Sets.newHashSet("map1", "Sink: sink1"),
					Sets.newHashSet("map1"),
					taskonfig, streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Filter1) -+
		 *          (   |              )  | -> CHAIN(Map1 -> Sink1          )
		 *          (   |_             ) -+         (  |_ -+                )
		 *          (   |              )            (     | -> Map2 -> Sink2)
		 *          (   |_             ) ->         (     -+                )
		 * </pre>
		 */
		@Test
		public void testTwoTrianglesTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> map1 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? source1.rescale() : source1)
				.map(new NoOpCoMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			DataStream<String> map2 = map1.connect(source1).map(new NoOpCoMapFunction()).name("map2");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN(Source1 -> Filter1) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("Source: source1", "map2"),
						Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Sets.newHashSet("Source: source1", "filter1"),
					Sets.newHashSet("Source: source1"),
					taskonfig, streamGraph);
			}

			// CHAIN([Map1 -> (Sink1, Map2), Map2 -> Sink2 ]) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(3, vertex.getInputs().size());
				assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("Source: source1", "map2"),
						Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Collections.emptyList(),
					Sets.newHashSet("map1", "Sink: sink1", "map2", "Sink: sink2"),
					Sets.newHashSet("map1", "map2"),
					taskonfig, streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1              ) -+
		 *          (   |_   -+           )  | -> CHAIN(Map1 -> Sink1)
		 *          (         | -> Filter1) -+
		 *          (Source2 -+           )
		 * </pre>
		 */
		@Test
		public void testMultiHeadTriangleTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> filter1 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("filter1");

			DataStream<String> map1 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? source1.rescale() : source1)
				.map(new NoOpCoMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN((Source1, Source2) -> Filter1) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Sets.newHashSet("Source: source1", "Source: source2", "filter1"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					taskonfig, streamGraph);
			}

			// CHAIN(Map1 -> Sink1) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Collections.emptyList(),
					Sets.newHashSet("map1", "Sink: sink1"),
					Sets.newHashSet("map1"),
					taskonfig, streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Filter1       ) -+
		 *          (   |                     )  | -> CHAIN(Process1 -> Sink1)
		 *          (   |_  -> Map1 -> Filter2) -+
		 * </pre>
		 */
		@Test
		public void testRhombusTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> filter2 = source1.map(new NoOpMapFunction()).name("map1")
				.filter(new NoOpFilterFunction()).name("filter2");

			DataStream<String> process1 = (breakingOffType == 1 || breakingOffType == 3 ? filter2.rescale() : filter2)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.process(new NoOpCoProcessFuntion()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN([Source1 -> (Filter1, Map1), Map1 -> Filter2]) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Sets.newHashSet("Source: source1", "map1", "filter1", "filter2"),
					Sets.newHashSet("Source: source1"),
					taskonfig, streamGraph);
			}

			// CHAIN(Process1 -> Sink1) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("process1", "Sink: sink1"),
					Sets.newHashSet("process1"),
					taskonfig, streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Filter1       ) -+
		 *          (   |                     )  | -> CHAIN(Process1 -> Sink1             )
		 *          (   |_  -> Map1 -> Filter2) -+         (    |_ -+                     )
		 *          (   |                     )            (        | -> Process2 -> Sink2)
		 *          (   |_                    ) ->         (       -+                     )
		 * </pre>
		 */
		@Test
		public void testTwoRhombusesTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> filter2 = source1.map(new NoOpMapFunction()).name("map1")
				.filter(new NoOpFilterFunction()).name("filter2");

			DataStream<String> process1 = (breakingOffType == 1 || breakingOffType == 3 ? filter2.rescale() : filter2)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.process(new NoOpCoProcessFuntion()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			DataStream<String> map2 = process1.connect(source1).process(new NoOpCoProcessFuntion()).name("process2");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN([Source1 -> (Filter1, Map1), Map1 -> Filter2]) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(SourceStreamTaskV2.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("Source: source1", "process2"),
						Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Sets.newHashSet("Source: source1", "map1", "filter1", "filter2"),
					Sets.newHashSet("Source: source1"),
					taskonfig, streamGraph);
			}

			// CHAIN([Process1 -> (Sink1, Process2), Process2 -> Sink2]) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(3, vertex.getInputs().size());
				assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("Source: source1", "process2"),
						Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("process1", "Sink: sink1", "process2", "Sink: sink2"),
					Sets.newHashSet("process1", "process2"),
					taskonfig, streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Filter1        ) -+
		 *          (   |                      )  | -> CHAIN(Process1 -> Sink1)
		 *          (   |_   -> Map1 -> Filter2) -+
		 *          (                     |    )
		 *          (Source2     ->      _|    )
		 * </pre>
		 */
		@Test
		public void testMultiHeadRhombusTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");

			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> filter2 = map1.connect(source2).process(new NoOpCoProcessFuntion()).name("filter2");

			DataStream<String> process1 = (breakingOffType == 1 || breakingOffType == 3 ? filter2.rescale() : filter2)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.process(new NoOpCoProcessFuntion()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(2, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN([Source1 -> (Filter1, Map1), (Map1, Source2) -> Filter2]) vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Sets.newHashSet("Source: source1", "Source: source2", "map1", "filter1", "filter2"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					taskonfig, streamGraph);
			}

			// CHAIN(Process1 -> Sink1) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter2", "process1"), Tuple2.of("filter1", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("process1", "Sink: sink1"),
					Sets.newHashSet("process1"),
					taskonfig, streamGraph);
			}
		}


		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     CHAIN(Source1 -> Map0 -> Map1              ) -+
		 *          (           |                         )  |
		 *          (           |_   -> Map2 -+           )  | -> CHAIN(Process1 -> Sink1)
		 *          (                         | -> Filter1) -+
		 *          (Source2 -> Map3         -+           )  | -> CHAIN(Map5 -> Sink2)
		 *          (            |_                       ) -+
		 *          (            |_  -> Map4              ) -> CHAIN(Filter2 -> Sink3)
		 * </pre>
		 */
		@Test
		public void testComplexTopology() throws Exception {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
			env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
			env.setMultiHeadChainMode(true);

			ClassLoader cl = getClass().getClassLoader();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map0 = source1.map(new NoOpMapFunction()).name("map0");
			DataStream<String> map1 = map0.map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = map0.map(new NoOpMapFunction()).name("map2");

			DataStream<String> map3 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2")
				.map(new NoOpMapFunction()).name("map3");
			DataStream<String> filter1 = map2.connect(map3).process(new NoOpCoProcessFuntion()).name("filter1");
			DataStream<String> map4 = map3.map(new NoOpMapFunction()).name("map4");

			DataStream<String> process1 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? map1.rescale() : map1)
				.process(new NoOpCoProcessFuntion()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			DataStream<String> map5 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? map3.rescale() : map3)
				.map(new NoOpCoMapFunction()).name("map5");
			map5.addSink(new NoOpSinkFunction()).name("sink2");

			map4.filter(new NoOpFilterFunction()).name("filter2").addSink(new NoOpSinkFunction()).name("sink3");

			StreamGraph streamGraph = env.getStreamGraph();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				if ("filter2".equals(node.getOperatorName())) {
					node.getOperator().setChainingStrategy(ChainingStrategy.HEAD);
				} else {
					node.getOperator().setChainingStrategy(ChainingStrategy.ALWAYS);
				}
			}

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(4, jobGraph.getNumberOfVertices());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			// CHAIN1 vertex
			{
				JobVertex vertex = verticesSorted.get(0);
				assertEquals(0, vertex.getInputs().size());
				assertEquals(ArbitraryInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "process1"), Tuple2.of("map1", "process1"),
						Tuple2.of("filter1", "map5"), Tuple2.of("map3", "map5"), Tuple2.of("map4", "filter2")),
					Sets.newHashSet("Source: source1", "Source: source2", "map0", "map1", "map2", "map3", "map4", "filter1"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					taskonfig, streamGraph);
			}

			// CHAIN(Process1 -> Sink1) vertex
			{
				JobVertex vertex = verticesSorted.get(1);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "process1"), Tuple2.of("map1", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("process1", "Sink: sink1"),
					Sets.newHashSet("process1"),
					taskonfig, streamGraph);
			}

			// CHAIN(Map5 -> Sink2) vertex
			{
				JobVertex vertex = verticesSorted.get(2);
				assertEquals(2, vertex.getInputs().size());
				assertEquals(TwoInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "map5"), Tuple2.of("map3", "map5")),
					Collections.emptyList(),
					Sets.newHashSet("map5", "Sink: sink2"),
					Sets.newHashSet("map5"),
					taskonfig, streamGraph);
			}

			// CHAIN(Filter2 -> Sink3) vertex
			{
				JobVertex vertex = verticesSorted.get(3);
				assertEquals(1, vertex.getInputs().size());
				assertEquals(OneInputStreamTask.class, vertex.getInvokableClass(cl));

				StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
				verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("map4", "filter2")),
					Collections.emptyList(),
					Sets.newHashSet("filter2", "Sink: sink3"),
					Sets.newHashSet("filter2"),
					taskonfig, streamGraph);
			}
		}
	}

	// --------------------------------------------------------------------------------

	private static class NoOpSourceFunction implements ParallelSourceFunction<String> {

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}

	private static class NoOpSourceFunctionV2 implements ParallelSourceFunctionV2<String> {

		@Override
		public boolean isFinished() {
			return false;
		}

		@Override
		public SourceRecord<String> next() throws Exception {
			return null;
		}
	}

	private static class NoOpSinkFunction implements SinkFunction<String> {

	}

	private static class NoOpMapFunction implements MapFunction<String, String> {

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

	private static class NoOpCoMapFunction implements CoMapFunction<String, String, String> {

		@Override
		public String map1(String value) {
			return value;
		}

		@Override
		public String map2(String value) {
			return value;
		}
	}

	private static class NoOpCoProcessFuntion extends CoProcessFunction<String, String, String> {

		@Override
		public void processElement1(String value, Context ctx, Collector<String> out) {

		}

		@Override
		public void processElement2(String value, Context ctx, Collector<String> out) {

		}
	}

	private static class NoOpFilterFunction implements FilterFunction<String> {

		@Override
		public boolean filter(String value) throws Exception {
			return true;
		}
	}
}
