/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.ResourceConstraints;
import org.apache.flink.api.common.operators.StrictlyMatchingResourceConstraints;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceConstraintsOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for streaming resource constraints.
 */
@SuppressWarnings("serial")
public class ResourceConstraintTest  extends TestLogger {

	@Test
	public void testNotChain() {
		// --------- the program ---------

		ResourceConstraints rc1 = new StrictlyMatchingResourceConstraints();
		rc1.addConstraint("key1", "val1");
		ResourceConstraints rc2 = new StrictlyMatchingResourceConstraints();
		rc2.addConstraint("key2", "val2");
		rc2.addConstraint("key3", "val3");
		ResourceConstraints rc3 = new StrictlyMatchingResourceConstraints();
		rc3.addConstraint("key4", "val4");
		rc3.addConstraint("key5", "val5");
		rc3.addConstraint("key6", "val6");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, String>> input = env
			.setParallelism(1)
			.fromElements("a", "b", "c", "d", "e", "f")
			.map(new MapFunction<String, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(String value) {
					return new Tuple2<>(value, value);
				}
			}).setResourceConstraints(rc1);

		DataStream<Tuple2<String, String>> result = input
			.keyBy(0)
			.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(Tuple2<String, String> value) {
					return value;
				}
			}).setResourceConstraints(rc2);

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		}).setResourceConstraints(rc3);

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(4, jobGraph.getNumberOfVertices());
		assertEquals(null, verticesSorted.get(0).getResourceConstraints());
		assertTrue(rc1.equals(verticesSorted.get(1).getResourceConstraints()));
		assertTrue(rc2.equals(verticesSorted.get(2).getResourceConstraints()));
		assertTrue(rc3.equals(verticesSorted.get(3).getResourceConstraints()));
	}

	@Test
	public void testChainWithoutShuffle() {

		// --------- the program ---------

		ResourceConstraints rc1 = new StrictlyMatchingResourceConstraints();
		rc1.addConstraint("key1", "val1");
		ResourceConstraints rc2 = new StrictlyMatchingResourceConstraints();
		rc2.addConstraint("key2", "val2");
		rc2.addConstraint("key3", "val3");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, String>> input = env
			.setParallelism(1)
			.fromElements("a", "b", "c", "d", "e", "f")
			.setResourceConstraints(rc1)
			.map(new MapFunction<String, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(String value) {
					return new Tuple2<>(value, value);
				}
			}).setResourceConstraints(rc1);

		DataStream<Tuple2<String, String>> result = input
			.keyBy(0)
			.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(Tuple2<String, String> value) {
					return value;
				}
			}).setResourceConstraints(rc2);

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(3, jobGraph.getNumberOfVertices());
		assertTrue(rc1.equals(verticesSorted.get(0).getResourceConstraints()));
		assertTrue(rc2.equals(verticesSorted.get(1).getResourceConstraints()));
		assertEquals(null, verticesSorted.get(2).getResourceConstraints());
	}

	@Test
	public void testChainWithShuffle() {
		ResourceConstraints rc1 = new StrictlyMatchingResourceConstraints();
		rc1.addConstraint("key1", "val1");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});

		DataStream<Tuple2<Integer, Integer>> map = source.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		}).setResourceConstraints(rc1);

		// CHAIN(Source -> Map -> Filter)
		DataStream<Tuple2<Integer, Integer>> filter = map.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return false;
			}
		}).setResourceConstraints(rc1);

		DataStream<Tuple2<Integer, Integer>> reduce = filter.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
			}
		}).setResourceConstraints(rc1);

		DataStreamSink<Tuple2<Integer, Integer>> sink = reduce.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void invoke(Tuple2<Integer, Integer> value) throws Exception {
			}
		}).setResourceConstraints(rc1);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		assertEquals(3, jobGraph.getVerticesSortedTopologicallyFromSources().size());

		JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex mapFilterVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		JobVertex reduceSinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);

		assertEquals(null, sourceVertex.getResourceConstraints());
		assertTrue(rc1.equals(mapFilterVertex.getResourceConstraints()));
		assertTrue(rc1.equals(reduceSinkVertex.getResourceConstraints()));
	}

	@Test
	public void testDefaultResourceConstraints() {
		Map<String, String> defaultResourceConstraints = new HashMap<>();
		defaultResourceConstraints.put(ResourceConstraintsOptions.BLINK_RESOURCE_CONSTRAINT_PREFIX + "key1", "val1");
		defaultResourceConstraints.put(ResourceConstraintsOptions.BLINK_RESOURCE_CONSTRAINT_PREFIX + "key2", "val2");

		ResourceConstraints rc1 = new StrictlyMatchingResourceConstraints();
		rc1.addConstraint(ResourceConstraintsOptions.BLINK_RESOURCE_CONSTRAINT_PREFIX + "key1", "val1");
		rc1.addConstraint(ResourceConstraintsOptions.BLINK_RESOURCE_CONSTRAINT_PREFIX + "key2", "val2");

		Configuration flinkConf = new Configuration();
		flinkConf.addAll(defaultResourceConstraints);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});

		DataStream<Tuple2<Integer, Integer>> map = source.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		});

		// CHAIN(Source -> Map -> Filter)
		DataStream<Tuple2<Integer, Integer>> filter = map.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return false;
			}
		});

		DataStream<Tuple2<Integer, Integer>> reduce = filter.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
			}
		});

		DataStreamSink<Tuple2<Integer, Integer>> sink = reduce.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void invoke(Tuple2<Integer, Integer> value) throws Exception {
			}
		});

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph(), flinkConf);

		assertEquals(2, jobGraph.getVerticesSortedTopologicallyFromSources().size());

		JobVertex sourceMapFilterVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex reduceSinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

		assertTrue(rc1.equals(sourceMapFilterVertex.getResourceConstraints()));
		assertTrue(rc1.equals(reduceSinkVertex.getResourceConstraints()));
	}
}
