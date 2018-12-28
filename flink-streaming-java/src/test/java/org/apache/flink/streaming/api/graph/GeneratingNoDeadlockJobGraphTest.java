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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for generating no deadlock JobGraph.
 */
public class GeneratingNoDeadlockJobGraphTest {

	@Test
	public void testBasicGraph() throws Exception {
		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{});
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map1.addSink(new NoOpSinkFunction()).name("sink2");

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "RANDOM_ORDER";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{});
			}
		}
	}

	@Test
	public void testUnion() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = filter1.union(source1).map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{});
		}

		// case: double cross
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> map1 = source1.union(source2).map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = source1.union(source2).map(new NoOpMapFunction()).name("map2");
			map1.addSink(new NoOpSinkFunction()).name("sink1");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			testBase(env, new Integer[][]{});
		}
	}

	@Test
	public void testTriangleTopology() throws Exception {
		for (int i = 0; i < 4; i++) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = filter1.connect(source1).map(new NoOpCoMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			String msgPrefix = null;
			switch (i) {
				case 0:
					msgPrefix = "RANDOM_ORDER";

					testBase(env, new Integer[][]{});
					break;
				case 1:
					((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					msgPrefix = "INPUT1_FIRST";

					testBase(env, msgPrefix, new Integer[][]{
							new Integer[]{filter1.getId(), map1.getId()},
							new Integer[]{source1.getId(), map1.getId()}
					});
					break;
				case 2:
					((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
					msgPrefix = "INPUT2_FIRST";

					testBase(env, msgPrefix, new Integer[][]{
							new Integer[]{filter1.getId(), map1.getId()},
							new Integer[]{source1.getId(), map1.getId()}
					});
					break;
				case 3:
					((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					msgPrefix = "DYNAMIC_ORDER";

					testBase(env, msgPrefix, new Integer[][]{
							new Integer[]{filter1.getId(), map1.getId()},
							new Integer[]{source1.getId(), map1.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testCrossTopology() throws Exception {
		// case:
		{
			for (int i = 0; i < 3; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "ALL_RANDOM_ORDER";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "ALL_INPUT1_FIRST";
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "ALL_INPUT2_FIRST";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{});
			}
		}

		// case:
		{
			for (int i = 0; i < 3; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				switch (i) {
					case 0:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

						testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

						testBase(env, "INPUT2_INPUT1_FIRST", new Integer[][]{
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

						testBase(env, "ALL_DYNAMIC_ORDER", new Integer[][]{
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source1.getId(), map2.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
				}
			}
		}
	}

	@Test
	public void testDam() throws Exception {
		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> process1 = source1.transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> process2 = source2.transform(
						"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
				DataStream<String> map1 = process1.connect(process2).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map1.addSink(new NoOpSinkFunction()).name("sink2");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
				((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "RANDOM_ORDER";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{});
			}
		}
	}

	@Test
	public void testUnionAndTriangleTopology() throws Exception {
		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.union(source1).map(new NoOpMapFunction()).name("map1");
				DataStream<String> map2 = map1.connect(source1).map(new NoOpCoMapFunction()).name("map2");
				map2.addSink(new NoOpSinkFunction()).name("sink1");

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "RANDOM_ORDER";

						testBase(env, msgPrefix, new Integer[][]{});
						break;
					case 1:
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{map1.getId(), map2.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{map1.getId(), map2.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{filter1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
				}
			}
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> filter2 = source1.filter(new NoOpFilterFunction()).name("filter2");
				DataStream<String> map1 = filter1.connect(source1.union(filter2)).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "RANDOM_ORDER";

						testBase(env, msgPrefix, new Integer[][]{});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{filter1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{filter2.getId(), map1.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{filter1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{filter2.getId(), map1.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{filter1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{filter2.getId(), map1.getId()}
						});
						break;
				}
			}
		}
	}

	@Test
	public void testUnionAndCrossTopology() throws Exception {
		// case:
		{
			for (int i = 0; i < 2; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source0 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source0");
				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.union(source0).connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.union(source0).connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				String msgPrefix = null;
				switch (i) {
					case 0:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "ALL_INPUT1_FIRST";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "ALL_INPUT2_FIRST";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{});
			}
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source0 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source0");
				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.union(source0).connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.union(source0).connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				switch (i) {
					case 0:
						testBase(env, "ALL_RANDOM_ORDER", new Integer[][]{});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

						testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source0.getId(), map2.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

						testBase(env, "INPUT2_INPUT1_FIRST", new Integer[][]{
								new Integer[]{source0.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

						testBase(env, "ALL_DYNAMIC_ORDER", new Integer[][]{
								new Integer[]{source0.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source0.getId(), map2.getId()},
								new Integer[]{source1.getId(), map2.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
				}
			}
		}
	}

	@Test
	public void testTriangleAndCrossTopology() throws Exception {
		// case:
		{
			for (int i = 0; i < 2; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map0 = filter1.connect(source1).map(new NoOpCoMapFunction()).name("map0");
				((TwoInputTransformation) map0.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = map0.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = map0.connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				String msgPrefix = null;
				switch (i) {
					case 0:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "ALL_INPUT1_FIRST";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "ALL_INPUT2_FIRST";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{
						new Integer[]{filter1.getId(), map0.getId()},
						new Integer[]{source1.getId(), map0.getId()}
				});
			}
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map0 = filter1.connect(source1).map(new NoOpCoMapFunction()).name("map0");

				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = map0.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = map0.connect(source2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				switch (i) {
					case 0:
						testBase(env, "ALL_RANDOM_ORDER", new Integer[][]{});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

						testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
								new Integer[]{filter1.getId(), map0.getId()},
								new Integer[]{source1.getId(), map0.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{map0.getId(), map2.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

						testBase(env, "INPUT2_INPUT1_FIRST", new Integer[][]{
								new Integer[]{filter1.getId(), map0.getId()},
								new Integer[]{source1.getId(), map0.getId()},
								new Integer[]{map0.getId(), map1.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

						testBase(env, "ALL_DYNAMIC_ORDER", new Integer[][]{
								new Integer[]{filter1.getId(), map0.getId()},
								new Integer[]{source1.getId(), map0.getId()},
								new Integer[]{map0.getId(), map1.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{map0.getId(), map2.getId()},
								new Integer[]{source2.getId(), map2.getId()}
						});
						break;
				}
			}
		}
	}

	@Test
	public void testDamAndUnion() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			DataStream<String> map1 = process1.union(source1).map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}

		// case: double cross
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			DataStream<String> process2 = source2.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			DataStream<String> map1 = process1.union(source2).map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = source1.union(process2).map(new NoOpMapFunction()).name("map2");
			map1.addSink(new NoOpSinkFunction()).name("sink1");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}
	}

	@Test
	public void testDamAndTriangleTopology() throws Exception {
		// case: FULL_DAM
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> process1 = source1.rescale().transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> map1 = process1.connect(source1).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
						});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{process1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{process1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{process1.getId(), map1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
				}

			}
		}

		// case: MATERIALIZING
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> process1 = source1.rescale().transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> map1 = process1.connect(source1).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.MATERIALIZING);

				String msgPrefix = null;
				switch (i) {
					case 0:
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
						});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "INPUT1_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "INPUT2_FIRST";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						msgPrefix = "DYNAMIC_ORDER";

						testBase(env, msgPrefix, new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source1.getId(), map1.getId()}
						});
						break;
				}

			}
		}

		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> process1 = source1.rescale().process(new NoOpProcessFuntion()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
			DataStream<String> map1 = source1.rescale().map(new NoOpMapFunction()).name("map1");

			DataStream<String> filter1 = map1.connect(source2).process(new NoOpCoProcessFuntion()).name("filter1");
			((TwoInputTransformation) filter1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			DataStream<String> process2 = process1.connect(filter1.rescale()).process(new NoOpCoProcessFuntion()).name("process2");
			((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			process2.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{
					new Integer[]{source1.getId(), process1.getId()},
					new Integer[]{source1.getId(), map1.getId()},
					new Integer[]{process1.getId(), process2.getId()},
					new Integer[]{filter1.getId(), process2.getId()}
			});
		}
	}

	@Test
	public void testDamAndCrossTopology() throws Exception {
		// case:
		{
			for (int i = 0; i < 2; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> process1 = source1.transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> process2 = source2.transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
				DataStream<String> map1 = process1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(process2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
				((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				String msgPrefix = null;
				switch (i) {
					case 0:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						msgPrefix = "ALL_INPUT1_FIRST";
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						msgPrefix = "ALL_INPUT2_FIRST";
						break;
				}

				testBase(env, msgPrefix, new Integer[][]{});
			}
		}

		// case:
		{
			for (int i = 0; i < 4; i++) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> process1 = source1.rescale().transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> process2 = source2.rescale().transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
				DataStream<String> map1 = process1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(process2).map(new NoOpCoMapFunction()).name("map2");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map2.addSink(new NoOpSinkFunction()).name("sink2");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
				((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				switch (i) {
					case 0:
						testBase(env, "ALL_DYNAMIC_ORDER", new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source2.getId(), process2.getId()},
						});
						break;
					case 1:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

						testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source2.getId(), process2.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source1.getId(), map2.getId()}
						});
						break;
					case 2:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

						testBase(env, "INPUT2_INPUT1_FIRST", new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source2.getId(), process2.getId()},
								new Integer[]{process1.getId(), map1.getId()},
								new Integer[]{process2.getId(), map2.getId()}
						});
						break;
					case 3:
						((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

						testBase(env, "ALL_DYNAMIC_ORDER", new Integer[][]{
								new Integer[]{source1.getId(), process1.getId()},
								new Integer[]{source2.getId(), process2.getId()},
								new Integer[]{process1.getId(), map1.getId()},
								new Integer[]{source2.getId(), map1.getId()},
								new Integer[]{source1.getId(), map2.getId()},
								new Integer[]{process2.getId(), map2.getId()}
						});
						break;
				}
			}
		}
	}

	private static StreamExecutionEnvironment createEnv() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(GeneratingNoDeadlockJobGraphTest.class.getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		return env;
	}

	private static void testBase(StreamExecutionEnvironment env, Integer[][] expect) {
		testBase(env, null, expect);
	}

	private static void testBase(StreamExecutionEnvironment env, String msgPrefix, Integer[][] expect) {
		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		List<Integer> sortedSourceIDs = streamGraph.getSourceIDs().stream()
				.sorted(
						Comparator.comparing((Integer id) -> {
							StreamOperator<?> operator = streamGraph.getStreamNode(id).getOperator();
							return operator == null || operator instanceof StreamSource ? 0 : 1;
						}).thenComparingInt(id -> id))
				.collect(Collectors.toList());

		List<StreamingJobGraphGenerator.ChainingStreamNode> sortedChainingNodes = StreamingJobGraphGenerator.sortTopologicalNodes(streamGraph, sortedSourceIDs);

		Map<Integer, StreamingJobGraphGenerator.ChainingStreamNode> chainingNodeMap = sortedChainingNodes.stream()
				.collect(Collectors.toMap(StreamingJobGraphGenerator.ChainingStreamNode::getNodeId, (o) -> o));

		StreamingJobGraphGenerator.splitUpInitialChains(chainingNodeMap, sortedChainingNodes, streamGraph);
		StreamingJobGraphGenerator.breakOffChainForNoDeadlock(chainingNodeMap, sortedChainingNodes, streamGraph);

		Set<StreamEdge> breakEdgeSet = new HashSet<>();
		for (StreamingJobGraphGenerator.ChainingStreamNode chainingNode : chainingNodeMap.values()) {
			Integer nodeId = chainingNode.getNodeId();
			StreamNode node = streamGraph.getStreamNode(nodeId);
			for (StreamEdge edge : node.getInEdges()) {
				Integer sourceId = edge.getSourceId();
				if (!chainingNode.isChainTo(sourceId)) {
					breakEdgeSet.add(edge);
				}
			}
		}

		for (int i = 0; i < expect.length; i++) {
			Integer sourceId = expect[i][0];
			Integer targetId = expect[i][1];
			assertTrue(
					String.format("%sThe edge(%s -> %s) is not broken.",
							(msgPrefix != null && !msgPrefix.isEmpty()) ? msgPrefix + ": " : "",
							streamGraph.getStreamNode(sourceId).getOperatorName(),
							streamGraph.getStreamNode(targetId).getOperatorName()),
					breakEdgeSet.contains(streamGraph.getStreamEdges(sourceId, targetId).get(0)));
		}
		assertEquals((msgPrefix != null ? msgPrefix + ":" : "") +
				"The size of broken edges is not equal.", expect.length, breakEdgeSet.size());
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class NoOpSourceFunctionV2 implements ParallelSourceFunctionV2<String> {

		@Override
		public boolean isFinished() {
			return false;
		}

		@Override
		public SourceRecord<String> next() throws Exception {
			return null;
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

	private static class NoOpProcessFuntion extends ProcessFunction<String, String> {

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

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

	private static class NoOpFilterFunction implements FilterFunction<String> {

		@Override
		public boolean filter(String value) throws Exception {
			return true;
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

	private static class NoOpOneInputStreamOperator extends AbstractStreamOperator<String>
			implements OneInputStreamOperator<String, String> {

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {

		}

		@Override
		public void endInput() throws Exception {

		}
	}
}
