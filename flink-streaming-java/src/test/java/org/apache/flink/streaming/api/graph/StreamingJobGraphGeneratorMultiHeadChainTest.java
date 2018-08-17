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
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigSnapshot;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;

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
	 * <pre>
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> source2 = env.addSource(new NoOpSourceFunction()).name("source2");

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

		StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
		verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
			Collections.emptyList(),
			Collections.emptyList(),
			Sets.newHashSet("Source: source1", "Source: source2", "map1", "Sink: sink1", "Sink: sink2"),
			Sets.newHashSet("Source: source1", "Source: source2"),
			taskonfig);
	}

	/**
	 * Tests the following topology.
	 *
	 * <pre>
	 *     CHAIN(Source1 -> Filter1) -+
	 *          (   |              )  | -> CHAIN(Join1 -> Sink1)
	 *          (   |__  ->        ) -+
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

		DataStream<String> join1 = filter1.connect(source1).process(new NoOpCoProcessFuntion()).name("join1");
		join1.addSink(new NoOpSinkFunction()).name("sink1");

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

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("Source: source1", "join1")),
				Sets.newHashSet("Source: source1", "filter1"),
				Sets.newHashSet("Source: source1"),
				taskonfig);
		}

		// CHAIN(Join1 -> Sink1) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("Source: source1", "join1")),
				Collections.emptyList(),
				Sets.newHashSet("join1", "Sink: sink1"),
				Sets.newHashSet("join1"),
				taskonfig);
		}
	}


	/**
	 * Tests the following topology.
	 *
	 * <pre>
	 *     CHAIN( Source1 -> +-  ->) -+
	 *          (            |     )  | -> CHAIN(Join1 -> Sink1)
	 *          (Source2 -> Filter1) -+
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> source2 = env.addSource(new NoOpSourceFunction()).name("source2");
		DataStream<String> filter1 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("filter1");

		DataStream<String> join1 = source1.connect(filter1).process(new NoOpCoProcessFuntion()).name("join1");
		join1.addSink(new NoOpSinkFunction()).name("sink1");

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

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "join1"), Tuple2.of("filter1", "join1")),
				Sets.newHashSet("Source: source1", "Source: source2", "filter1"),
				Sets.newHashSet("Source: source1", "Source: source2"),
				taskonfig);
		}

		// CHAIN(Join1 -> Sink1) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("Source: source1", "join1"), Tuple2.of("filter1", "join1")),
				Collections.emptyList(),
				Sets.newHashSet("join1", "Sink: sink1"),
				Sets.newHashSet("join1"),
				taskonfig);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <pre>
	 *     CHAIN(Source1 -> Filter1) -+
	 *          (   |              )  | -> CHAIN(Join1 -> Sink1)
	 *          (   |_   -> Filter2) -+
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
		DataStream<String> filter2 = source1.filter(new NoOpFilterFunction()).name("filter2");

		DataStream<String> join1 = filter1.connect(filter2)
			.keyBy(new NoOpKeySelector(), new NoOpKeySelector()).process(new NoOpCoProcessFuntion()).name("join1");
		join1.addSink(new NoOpSinkFunction()).name("sink1");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(2, jobGraph.getNumberOfVertices());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		// CHAIN(Source1 -> Filter[1,2]) vertex
		{
			JobVertex vertex = verticesSorted.get(0);
			assertEquals(0, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1")),
				Sets.newHashSet("Source: source1", "filter1", "filter2"),
				Sets.newHashSet("Source: source1"),
				taskonfig);
		}

		// CHAIN(Join1 -> Sink1) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1")),
				Collections.emptyList(),
				Sets.newHashSet("join1", "Sink: sink1"),
				Sets.newHashSet("join1"),
				taskonfig);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <pre>
	 *     CHAIN(Source1 -> Filter1        ) -+
	 *          (   |                      )  | -> CHAIN(Join1 -> Sink1)
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
		DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");

		DataStream<String> source2 = env.addSource(new NoOpSourceFunction()).name("source2");
		DataStream<String> filter2 = map1.connect(source2).process(new NoOpCoProcessFuntion()).name("filter2");

		DataStream<String> join1 = filter1.connect(filter2)
			.keyBy(new NoOpKeySelector(), new NoOpKeySelector()).process(new NoOpCoProcessFuntion()).name("join1");
		join1.addSink(new NoOpSinkFunction()).name("sink1");

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

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1")),
				Sets.newHashSet("Source: source1", "Source: source2", "map1", "filter1", "filter2"),
				Sets.newHashSet("Source: source1", "Source: source2"),
				taskonfig);
		}

		// CHAIN(Join1 -> Sink1) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1")),
				Collections.emptyList(),
				Sets.newHashSet("join1", "Sink: sink1"),
				Sets.newHashSet("join1"),
				taskonfig);
		}
	}


	/**
	 * Tests the following topology.
	 *
	 * <pre>
	 *     CHAIN(Source1 -> Map1 -> Filter1    ) -+
	 *          (   |                          )  | -> CHAIN(Join1 -> Sink1)
	 *          (   |_   -> Map2 -> Filter2    ) -+
	 *          (                     |        )  | -> CHAIN(Join2 -> Sink2)
	 *          (Source2 -> Map3 ->  +-  ->    ) -+
	 *          (            |                 )
	 *          (            |__  -> Map4      ) -> CHAIN(Filter3 -> Sink3)
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

		DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
		DataStream<String> filter1 =  map1.filter(new NoOpFilterFunction()).name("filter1");
		DataStream<String> map2 = source1.map(new NoOpMapFunction()).name("map2");

		DataStream<String> map3 = env.addSource(new NoOpSourceFunction()).name("source2")
			.map(new NoOpMapFunction()).name("map3");

		DataStream<String> filter2 = map2.connect(map3).process(new NoOpCoProcessFuntion()).name("filter2");
		DataStream<String> join1 = filter1.connect(filter2)
			.keyBy(new NoOpKeySelector(), new NoOpKeySelector()).process(new NoOpCoProcessFuntion()).name("join1");
		join1.addSink(new NoOpSinkFunction()).name("sink1");

		DataStream<String> join2 = filter2.connect(map3)
			.keyBy(new NoOpKeySelector(), new NoOpKeySelector()).process(new NoOpCoProcessFuntion()).name("join2");
		join2.addSink(new NoOpSinkFunction()).name("sink2");

		DataStream<String> map4 = map3.map(new NoOpMapFunction()).name("map4");
		map4.filter(new NoOpFilterFunction()).name("filter3")
			.addSink(new NoOpSinkFunction()).name("sink3");

		StreamGraph streamGraph = env.getStreamGraph();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			if ("filter3".equals(node.getOperatorName())) {
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

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1"),
					Tuple2.of("filter2", "join2"), Tuple2.of("map3", "join2"), Tuple2.of("map4", "filter3")),
				Sets.newHashSet("Source: source1", "Source: source2", "map1", "map2", "map3", "map4", "filter1", "filter2"),
				Sets.newHashSet("Source: source1", "Source: source2"),
				taskonfig);
		}

		// CHAIN(Join1 -> Sink1) vertex
		{
			JobVertex vertex = verticesSorted.get(1);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "join1"), Tuple2.of("filter2", "join1")),
				Collections.emptyList(),
				Sets.newHashSet("join1", "Sink: sink1"),
				Sets.newHashSet("join1"),
				taskonfig);
		}

		// CHAIN(Join2 -> Sink2) vertex
		{
			JobVertex vertex = verticesSorted.get(2);
			assertEquals(2, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter2", "join2"), Tuple2.of("map3", "join2")),
				Collections.emptyList(),
				Sets.newHashSet("join2", "Sink: sink2"),
				Sets.newHashSet("join2"),
				taskonfig);
		}

		// CHAIN(Filter4 -> Sink3) vertex
		{
			JobVertex vertex = verticesSorted.get(3);
			assertEquals(1, vertex.getInputs().size());

			StreamTaskConfigSnapshot taskonfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("map4", "filter3")),
				Collections.emptyList(),
				Sets.newHashSet("filter3", "Sink: sink3"),
				Sets.newHashSet("filter3"),
				taskonfig);
		}
	}

	// --------------------------------------------------------------------------------

	private static class NoOpSourceFunction implements ParallelSourceFunction<String> {

		private static final long serialVersionUID = -5459224792698512636L;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
		}

		@Override
		public void cancel() {
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

	private static class NoOpKeySelector implements KeySelector<String, String> {

		@Override
		public String getKey(String value) throws Exception {
			return value;
		}
	}
}
