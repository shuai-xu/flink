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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.SerializedInputFormat;
import org.apache.flink.api.common.io.SerializedOutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.FormatUtil.FormatType;
import org.apache.flink.runtime.jobgraph.FormatUtil.MultiFormatStub;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.MultiInputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractOneInputSubstituteStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigSnapshot;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamingJobGraphGenerator}.
 */
@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorTest extends TestLogger {

	/**
	 * Tests basic chaining logic using the following topology.
	 *
	 * <pre>
	 *     fromElements -> CHAIN(Map1 -> Filter1) -+
	 *                          ( |             )  | -> CHAIN(Map2 -> Print1)
	 *                          ( |_  -> Filter2) -+
	 *                                      |_ -> Print2
	 * </pre>
	 */
	@Test
	public void testBasicChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(12345);
		env.setBufferTimeout(54321L);
		env.enableCheckpointing(10, CheckpointingMode.AT_LEAST_ONCE);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.registerCachedFile("file:///testCachedFile", "test");
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));

		ClassLoader cl = getClass().getClassLoader();

		DataStream<Integer> sourceMap = env.fromElements(1, 2, 3).name("source1")
			.map((value) -> value).name("map1").setParallelism(66);

		DataStream<Integer> filter1 = sourceMap.filter((value) -> false).name("filter1").setParallelism(66);
		DataStream<Integer> filter2 = sourceMap.filter((value) -> false).name("filter2").setParallelism(66);

		filter1.connect(filter2).map(new CoMapFunction<Integer, Integer, Integer>() {
			@Override
			public Integer map1(Integer value) {
				return value;
			}

			@Override
			public Integer map2(Integer value) {
				return value;
			}
		}).name("map2").setParallelism(66).print().name("print1").setParallelism(66);
		filter2.print().name("print2").setParallelism(77);

		StreamGraph streamGraph = env.getStreamGraph();
		Map<String, StreamNode> streamNodeMap = new HashMap<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			streamNodeMap.put(node.getOperatorName(), node);
		}
		streamNodeMap.get("Source: source1").setInputFormat(new SerializedInputFormat());
		streamNodeMap.get("filter1").setOutputFormat(new SerializedOutputFormat());
		streamNodeMap.get("filter2").setOutputFormat(new SerializedOutputFormat());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(12345, jobGraph.getSerializedExecutionConfig().deserializeValue(getClass().getClassLoader()).getParallelism());
		assertEquals("file:///testCachedFile", jobGraph.getJobConfiguration().getString("DISTRIBUTED_CACHE_FILE_PATH_1", null));
		assertEquals(ScheduleMode.EAGER,
			ScheduleMode.valueOf(jobGraph.getSchedulingConfiguration().getString(ScheduleMode.class.getName(), null)));

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(4, verticesSorted.size());

		// Source0 vertex
		{
			JobVertex sourceVertex = verticesSorted.get(0);

			assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
			assertEquals(0, sourceVertex.getInputs().size());
			assertEquals(MultiInputOutputFormatVertex.class, sourceVertex.getClass());

			MultiFormatStub formatStub = new MultiFormatStub(new TaskConfig(sourceVertex.getConfiguration()), cl);
			Iterator<Pair<OperatorID, InputFormat>> inputFormatIterator = formatStub.getFormat(FormatType.INPUT);
			assertEquals(SerializedInputFormat.class, inputFormatIterator.next().getValue().getClass());
			assertFalse(inputFormatIterator.hasNext());
			assertFalse(formatStub.getFormat(FormatType.OUTPUT).hasNext());

			StreamTaskConfigSnapshot sourceTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(sourceVertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				Sets.newHashSet("Source: source1"),
				Sets.newHashSet("Source: source1"),
				sourceTaskConfig);

			StreamConfig sourceConfig = sourceTaskConfig.getChainedHeadNodeConfigs().get(0);
			verifyChainedNode("Source: source1", true, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				54321L,
				sourceConfig, cl);
		}

		// CHAIN(Map1 -> Filter[1,2]) vertex
		{
			JobVertex map1FilterVertex = verticesSorted.get(1);

			assertEquals(ResultPartitionType.PIPELINED_BOUNDED, map1FilterVertex.getInputs().get(0).getSource().getResultType());
			assertEquals(1, map1FilterVertex.getInputs().size());
			assertEquals(MultiInputOutputFormatVertex.class, map1FilterVertex.getClass());

			MultiFormatStub formatStub = new MultiFormatStub(new TaskConfig(map1FilterVertex.getConfiguration()), cl);
			assertFalse(formatStub.getFormat(FormatType.INPUT).hasNext());
			Iterator<Pair<OperatorID, OutputFormat>> outputFormatIterator = formatStub.getFormat(FormatType.OUTPUT);
			assertEquals(SerializedOutputFormat.class, outputFormatIterator.next().getValue().getClass());
			assertEquals(SerializedOutputFormat.class, outputFormatIterator.next().getValue().getClass());
			assertFalse(outputFormatIterator.hasNext());

			StreamTaskConfigSnapshot mapFilterTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(map1FilterVertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				Lists.newArrayList(Tuple2.of("filter1", "map2"), Tuple2.of("filter2", "map2"), Tuple2.of("filter2", "Sink: print2")),
				Sets.newHashSet("map1", "filter1", "filter2"),
				Sets.newHashSet("map1"),
				mapFilterTaskConfig);

			StreamConfig mapConfig = mapFilterTaskConfig.getChainedHeadNodeConfigs().get(0);
			verifyChainedNode("map1", true, false, 1,
				Lists.newArrayList(Tuple2.of("map1", "filter1"), Tuple2.of("map1", "filter2")),
				Collections.emptyList(),
				54321L,
				mapConfig, cl);

			List<StreamEdge> mapChainedOutEdges = mapConfig.getChainedOutputs(cl);

			StreamConfig filter1Config = mapFilterTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(0).getTargetId());
			verifyChainedNode("filter1", false, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "map2")),
				54321L,
				filter1Config, cl);

			StreamConfig filter2Config = mapFilterTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(1).getTargetId());
			verifyChainedNode("filter2", false, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter2", "map2"), Tuple2.of("filter2", "Sink: print2")),
				54321L,
				filter2Config, cl);
		}

		// CHAIN(Map2 -> Print1) vertex
		{
			JobVertex map2PrintVertex = verticesSorted.get(2);

			assertEquals(2, map2PrintVertex.getInputs().size());
			assertEquals(JobVertex.class, map2PrintVertex.getClass());

			StreamTaskConfigSnapshot mapPrintTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(map2PrintVertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "map2"), Tuple2.of("filter2", "map2")),
				Collections.emptyList(),
				Sets.newHashSet("map2", "Sink: print1"),
				Sets.newHashSet("map2"),
				mapPrintTaskConfig);

			StreamConfig mapConfig = mapPrintTaskConfig.getChainedHeadNodeConfigs().get(0);
			verifyChainedNode("map2", true, false, 2,
				Lists.newArrayList(Tuple2.of("map2", "Sink: print1")),
				Collections.emptyList(),
				54321L,
				mapConfig, cl);

			List<StreamEdge> mapChainedOutEdges = mapConfig.getChainedOutputs(cl);

			StreamConfig printConfig = mapPrintTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(0).getTargetId());
			verifyChainedNode("Sink: print1", false, true, 0,
				Collections.emptyList(),
				Collections.emptyList(),
				54321L,
				printConfig, cl);
		}

		// Print2 vertex
		{
			JobVertex print2Vertex = verticesSorted.get(3);

			assertEquals(1, print2Vertex.getInputs().size());
			assertEquals(JobVertex.class, print2Vertex.getClass());

			StreamTaskConfigSnapshot printTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(print2Vertex.getConfiguration()), cl);
			verifyVertex(TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter2", "Sink: print2")),
				Collections.emptyList(),
				Sets.newHashSet("Sink: print2"),
				Sets.newHashSet("Sink: print2"),
				printTaskConfig);

			StreamConfig printConfig = printTaskConfig.getChainedHeadNodeConfigs().get(0);
			verifyChainedNode("Sink: print2", true, true, 1,
				Collections.emptyList(),
				Collections.emptyList(),
				54321L,
				printConfig, cl);
		}
	}

	@Test
	public void testDisableOperatorChaining() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// fromElements -> Map -> Print
		env.fromElements(1, 2, 3).name("source1")
			.map((MapFunction<Integer, Integer>) value -> value).name("map1")
			.print().name("print1");

		// chained
		{
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());
		}

		// disable chaining
		{
			env.disableOperatorChaining();
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			JobVertex mapVertex = verticesSorted.get(1);
			JobVertex printVertex = verticesSorted.get(2);

			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals("map1", mapVertex.getName());
			assertEquals("Sink: print1", printVertex.getName());

			assertEquals(1, sourceVertex.getParallelism());
			assertEquals(5, mapVertex.getParallelism());
			assertEquals(5, printVertex.getParallelism());
		}
	}

	@Test
	public void testBlockingEdgeNotChained() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// fromElements -> Map -> Print
		env.fromElements(1, 2, 3).name("source1")
			.map((MapFunction<Integer, Integer>) value -> value).name("map1")
			.print().name("print1");

		// chained
		{
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());
		}

		// set blocking edge
		{
			StreamGraph streamGraph = env.getStreamGraph();
			Map<String, StreamNode> streamNodeMap = new HashMap<>();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				streamNodeMap.put(node.getOperatorName(), node);
			}
			List<StreamEdge> streamEdges = streamNodeMap.get("map1").getOutEdges();
			assertEquals(1, streamEdges.size());
			streamEdges.get(0).setResultPartitionType(ResultPartitionType.BLOCKING);

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			JobVertex mapVertex = verticesSorted.get(1);
			JobVertex printVertex = verticesSorted.get(2);

			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals("map1", mapVertex.getName());
			assertEquals("Sink: print1", printVertex.getName());

			assertEquals(1, sourceVertex.getParallelism());
			assertEquals(5, mapVertex.getParallelism());
			assertEquals(5, printVertex.getParallelism());

			assertEquals(1, mapVertex.getProducedDataSets().size());
			assertEquals(ResultPartitionType.BLOCKING, mapVertex.getProducedDataSets().get(0).getResultType());
		}
	}

	@Test
	public void testSubstituteStreamOperatorChaining() {
		// chained
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(5);

			// fromElements -> Map -> Print
			TestSubstituteStreamOperator substituteOperator = new TestSubstituteStreamOperator<>(
				new StreamMap<>((MapFunction<Integer, Integer>) value -> value));
			substituteOperator.setChainingStrategy(ChainingStrategy.ALWAYS);

			env.fromElements(1, 2, 3).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1")
				.transform("operator1",
					BasicTypeInfo.INT_TYPE_INFO,
					substituteOperator)
				.print().name("print1");

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());
		}

		// not chained
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(5);

			// fromElements -> Map -> Print
			TestSubstituteStreamOperator substituteOperator = new TestSubstituteStreamOperator<>(
				new StreamMap<>((MapFunction<Integer, Integer>) value -> value));
			substituteOperator.setChainingStrategy(ChainingStrategy.HEAD);

			env.fromElements(1, 2, 3).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1")
				.transform("operator1",
					BasicTypeInfo.INT_TYPE_INFO,
					substituteOperator)
				.print().name("print1");

			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());
		}
	}

	@Test
	public void testParallelismOneNotChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
				.fromElements("a", "b", "c", "d", "e", "f")
				.map(new MapFunction<String, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(String value) {
						return new Tuple2<>(value, value);
					}
				});

		DataStream<Tuple2<String, String>> result = input
				.keyBy(0)
				.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(Tuple2<String, String> value) {
						return value;
					}
				});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, verticesSorted.get(0).getParallelism());
		assertEquals(1, verticesSorted.get(1).getParallelism());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapSinkVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapSinkVertex.getInputs().get(0).getSource().getResultType());
	}

	/**
	 * Tests that disabled checkpointing sets the checkpointing interval to Long.MAX_VALUE.
	 */
	@Test
	public void testDisabledCheckpointing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamGraph streamGraph = new StreamGraph(env.getConfig(),
			env.getCheckpointConfig(),
			env.getParallelism(),
			env.getBufferTimeout(),
			ResultPartitionType.PIPELINED_BOUNDED,
			DataPartitionerType.REBALANCE);
		assertFalse("Checkpointing enabled", streamGraph.getCheckpointConfig().isCheckpointingEnabled());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		JobCheckpointingSettings snapshottingSettings = jobGraph.getCheckpointingSettings();
		assertEquals(Long.MAX_VALUE, snapshottingSettings.getCheckpointCoordinatorConfiguration().getCheckpointInterval());
	}

	/**
	 * Verifies that the chain start/end is correctly set.
	 */
	@Test
	public void testChainStartEndSetting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> CHAIN(Map -> Print)
		env.fromElements(1, 2, 3).setParallelism(1)
			.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value;
				}
			}).setParallelism(2)
			.print().setParallelism(2);
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapPrintVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapPrintVertex.getInputs().get(0).getSource().getResultType());

		ClassLoader cl = getClass().getClassLoader();

		StreamTaskConfigSnapshot sourceTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(sourceVertex.getConfiguration()), cl);
		StreamTaskConfigSnapshot mapPrintTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(mapPrintVertex.getConfiguration()), cl);

		StreamConfig sourceConfig = sourceTaskConfig.getChainedHeadNodeConfigs().get(0);
		StreamConfig mapConfig = mapPrintTaskConfig.getChainedHeadNodeConfigs().get(0);
		List<StreamEdge> mapOutEdges = mapConfig.getChainedOutputs(cl);
		StreamConfig printConfig = mapPrintTaskConfig.getChainedNodeConfigs().get(mapOutEdges.get(0).getTargetId());

		assertTrue(sourceConfig.isChainStart());
		assertTrue(sourceConfig.isChainEnd());

		assertTrue(mapConfig.isChainStart());
		assertFalse(mapConfig.isChainEnd());

		assertFalse(printConfig.isChainStart());
		assertTrue(printConfig.isChainEnd());
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers source and sink cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForChainedSourceSink() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder().setCpuCores(0.1).setHeapMemoryInMB(100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder().setCpuCores(0.2).setHeapMemoryInMB(200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder().setCpuCores(0.3).setHeapMemoryInMB(300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder().setCpuCores(0.4).setHeapMemoryInMB(400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder().setCpuCores(0.5).setHeapMemoryInMB(500).build();

		Method opMethod = SingleOutputStreamOperator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataStreamSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});
		opMethod.invoke(source, resource1);

		DataStream<Tuple2<Integer, Integer>> map = source.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		});
		opMethod.invoke(map, resource2);

		// CHAIN(Source -> Map -> Filter)
		DataStream<Tuple2<Integer, Integer>> filter = map.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return false;
			}
		});
		opMethod.invoke(filter, resource3);

		DataStream<Tuple2<Integer, Integer>> reduce = filter.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
			}
		});
		opMethod.invoke(reduce, resource4);

		DataStreamSink<Tuple2<Integer, Integer>> sink = reduce.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void invoke(Tuple2<Integer, Integer> value) throws Exception {
			}
		});
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		JobVertex sourceMapFilterVertex = verticesSorted.get(0);
		JobVertex reduceSinkVertex = verticesSorted.get(1);

		assertTrue(sourceMapFilterVertex.getMinResources().equals(resource1.merge(resource2).merge(resource3)));
		assertTrue(reduceSinkVertex.getPreferredResources().equals(resource4.merge(resource5)));
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers middle chaining and iteration cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForIteration() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder().setCpuCores(0.1).setHeapMemoryInMB(100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder().setCpuCores(0.2).setHeapMemoryInMB(200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder().setCpuCores(0.3).setHeapMemoryInMB(300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder().setCpuCores(0.4).setHeapMemoryInMB(400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder().setCpuCores(0.5).setHeapMemoryInMB(500).build();

		Method opMethod = SingleOutputStreamOperator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataStreamSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.addSource(new ParallelSourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		}).name("test_source");
		opMethod.invoke(source, resource1);

		IterativeStream<Integer> iteration = source.iterate(3000);
		opMethod.invoke(iteration, resource2);

		DataStream<Integer> flatMap = iteration.flatMap(new FlatMapFunction<Integer, Integer>() {
			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value);
			}
		}).name("test_flatMap");
		opMethod.invoke(flatMap, resource3);

		// CHAIN(flatMap -> Filter)
		DataStream<Integer> increment = flatMap.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return false;
			}
		}).name("test_filter");
		opMethod.invoke(increment, resource4);

		DataStreamSink<Integer> sink = iteration.closeWith(increment).addSink(new SinkFunction<Integer>() {
			@Override
			public void invoke(Integer value) throws Exception {
			}
		}).disableChaining().name("test_sink");
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getName().contains("test_source")) {
				assertTrue(jobVertex.getMinResources().equals(resource1));
			} else if (jobVertex.getName().contains("Iteration_Source")) {
				assertTrue(jobVertex.getPreferredResources().equals(resource2));
			} else if (jobVertex.getName().contains("test_flatMap")) {
				assertTrue(jobVertex.getMinResources().equals(resource3.merge(resource4)));
			} else if (jobVertex.getName().contains("Iteration_Tail")) {
				assertTrue(jobVertex.getPreferredResources().equals(ResourceSpec.DEFAULT));
			} else if (jobVertex.getName().contains("test_sink")) {
				assertTrue(jobVertex.getMinResources().equals(resource5));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class TestSubstituteStreamOperator<IN, OUT> implements AbstractOneInputSubstituteStreamOperator<IN, OUT> {

		private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
		private final OneInputStreamOperator<IN, OUT> actualStreamOperator;

		TestSubstituteStreamOperator(OneInputStreamOperator<IN, OUT> actualStreamOperator) {
			this.actualStreamOperator = actualStreamOperator;
		}

		@Override
		public StreamOperator<OUT> getActualStreamOperator(ClassLoader cl) {
			return actualStreamOperator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy chainingStrategy) {
			this.chainingStrategy = chainingStrategy;
			this.actualStreamOperator.setChainingStrategy(chainingStrategy);
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return chainingStrategy;
		}
	}

	static void verifyVertex(
		TimeCharacteristic expectedTimeChar,
		boolean expectedCheckpointingEnabled,
		CheckpointingMode expectedCheckpointMode,
		Class<?> expectedStateBackend,
		List<Tuple2<String, String>> expectedInEdges,
		List<Tuple2<String, String>> expectedOutEdges,
		Set<String> expectedChainedNodes,
		Set<String> expectedHeadNodes,
		final StreamTaskConfigSnapshot vertexConfig) {

		assertEquals(expectedTimeChar, vertexConfig.getTimeCharacteristic());
		assertEquals(expectedCheckpointingEnabled, vertexConfig.isCheckpointingEnabled());
		assertEquals(expectedCheckpointMode, vertexConfig.getCheckpointMode());
		assertEquals(expectedStateBackend, vertexConfig.getStateBackend().getClass());
		assertEquals(expectedInEdges.size(), vertexConfig.getInStreamEdgesOfChain().size());
		assertEquals(expectedOutEdges.size(), vertexConfig.getOutStreamEdgesOfChain().size());
		assertEquals(expectedChainedNodes.size(), vertexConfig.getChainedNodeConfigs().size());
		assertEquals(expectedHeadNodes.size(), vertexConfig.getChainedHeadNodeIds().size());
		assertEquals(expectedHeadNodes.size(), vertexConfig.getChainedHeadNodeConfigs().size());

		List<String> expectedInEdgesKeys = expectedInEdges.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < vertexConfig.getInStreamEdgesOfChain().size(); i++) {
			StreamEdge edge = vertexConfig.getInStreamEdgesOfChain().get(i);
			assertEquals(expectedInEdgesKeys.get(i), edge.getSourceVertex().getOperatorName() + "|" + edge.getTargetVertex().getOperatorName());
		}

		List<String> expectedOutEdgesKeys = expectedOutEdges.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < vertexConfig.getOutStreamEdgesOfChain().size(); i++) {
			StreamEdge edge = vertexConfig.getOutStreamEdgesOfChain().get(i);
			assertEquals(expectedOutEdgesKeys.get(i), edge.getSourceVertex().getOperatorName() + "|" + edge.getTargetVertex().getOperatorName());
		}

		for (Map.Entry<Integer, StreamConfig> entry: vertexConfig.getChainedNodeConfigs().entrySet()) {
			assertTrue(expectedChainedNodes.contains(entry.getValue().getOperatorName()));
		}

		for (StreamConfig headNodeConfig : vertexConfig.getChainedHeadNodeConfigs()) {
			assertTrue(expectedHeadNodes.contains(headNodeConfig.getOperatorName()));
		}
	}

	private static void verifyChainedNode(
		String operatorName,
		boolean expectedChainStart,
		boolean expectedChainEnd,
		int expectedNumInputs,
		List<Tuple2<String, String>> expectedChainedOutputs,
		List<Tuple2<String, String>> expectedNonChainedOutputs,
		long expectedBufferTimeout,
		final StreamConfig nodeConfig,
		final ClassLoader classLoader) {

		final List<StreamEdge> chainedOutEdges = nodeConfig.getChainedOutputs(classLoader);
		final List<StreamEdge> nonChainedOutEdges = nodeConfig.getNonChainedOutputs(classLoader);

		assertEquals(operatorName, nodeConfig.getOperatorName());
		assertEquals(expectedChainStart, nodeConfig.isChainStart());
		assertEquals(expectedChainEnd, nodeConfig.isChainEnd());
		assertEquals(expectedNumInputs, nodeConfig.getNumberOfInputs());
		assertEquals(expectedNonChainedOutputs.size(), nodeConfig.getNumberOfOutputs());
		assertEquals(expectedChainedOutputs.size(), nodeConfig.getChainedOutputs(classLoader).size());
		assertEquals(expectedNonChainedOutputs.size(), nodeConfig.getNonChainedOutputs(classLoader).size());

		List<String> expectedChainedOutEdgesKeys = expectedChainedOutputs.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < chainedOutEdges.size(); i++) {
			StreamEdge edge = chainedOutEdges.get(i);
			assertEquals(expectedChainedOutEdgesKeys.get(i), edge.getSourceVertex().getOperatorName() + "|" + edge.getTargetVertex().getOperatorName());
		}

		List<String> expectedNonChainedOutEdgesKeys = expectedNonChainedOutputs.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < nonChainedOutEdges.size(); i++) {
			StreamEdge edge = nonChainedOutEdges.get(i);
			assertEquals(expectedNonChainedOutEdgesKeys.get(i), edge.getSourceVertex().getOperatorName() + "|" + edge.getTargetVertex().getOperatorName());
		}

		assertEquals(expectedBufferTimeout, nodeConfig.getBufferTimeout());
	}
}
