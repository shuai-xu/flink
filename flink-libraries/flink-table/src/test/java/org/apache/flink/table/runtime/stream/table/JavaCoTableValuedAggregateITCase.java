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

package org.apache.flink.table.runtime.stream.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.aggregate.TestInnerJoinFunc;
import org.apache.flink.table.plan.cost.Func0;
import org.apache.flink.table.runtime.utils.JavaStreamTestData;
import org.apache.flink.table.runtime.utils.TestingJavaRetractSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import scala.collection.JavaConversions;

/**
 * Integration tests for java Table API.
 */
public class JavaCoTableValuedAggregateITCase extends AbstractTestBase {

	@Test
	public void testCoAggApply() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataStream<Tuple3<Integer, Long, String>> ds1 =
			JavaStreamTestData.getSmall3TupleDataSet(env);
		Table in1 = tableEnv.fromDataStream(ds1, "a,b,c")
			.select("a as l1, c as l2");

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
			JavaStreamTestData.get5TupleDataStream(env);
		Table in2 = tableEnv.fromDataStream(ds2, "d,e,f,g,h")
			.select("d as r1, g as r2");

		TestInnerJoinFunc coTVAGGFunc = new TestInnerJoinFunc();
		tableEnv.registerFunction("coTVAGGFunc", coTVAGGFunc);
		Func0 func = new Func0();
		tableEnv.registerFunction("func", func);

		Table result = in1.connect(in2, "l1+2=func(r1)+2")
			.coAggApply("coTVAGGFunc(l1,l2)(r1,r2,r1+1)")
			.as("a, b, c, d, e");

		DataStream<Tuple2<Boolean, Row>> resultSet = tableEnv.toRetractStream(result, Row.class);
		TestingJavaRetractSink sink = new TestingJavaRetractSink();
		resultSet.addSink(sink);
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("3,1,Hi,1,Hallo,2,2");
		expected.add("4,2,Hello,2,Hallo Welt wie,3,3");
		expected.add("4,2,Hello,2,Hallo Welt,3,3");
		expected.add("5,3,Hello world,3,ABC,4,4");
		expected.add("5,3,Hello world,3,BCD,4,4");
		expected.add("5,3,Hello world,3,Hallo Welt wie gehts?,4,4");

		List<String> results = new ArrayList<>(
			JavaConversions.seqAsJavaList(sink.getRetractResults()));
		Collections.sort(expected);
		Collections.sort(results);

		Assert.assertEquals(expected, results);
	}
}
