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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.aggregate.SimpleTVAGG;
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
 * Integration tests for streaming Table API.
 */
public class JavaTableValuedAggregateITCase extends AbstractTestBase {

	@Test
	public void testAggApply() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataStream<Tuple3<Integer, Long, String>> ds =
			JavaStreamTestData.getSmall3TupleDataSet(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c");

		SimpleTVAGG fun = new SimpleTVAGG();
		tableEnv.registerFunction("fun", fun);

		Table result = in.groupBy("b").aggApply("fun(a)").select("b, f0");

		DataStream<Tuple2<Boolean, Row>> resultSet = tableEnv.toRetractStream(result, Row.class);
		TestingJavaRetractSink sink = new TestingJavaRetractSink();
		resultSet.addSink(sink);
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1");
		expected.add("1,1");
		expected.add("2,1");
		expected.add("2,5");

		List<String> results = new ArrayList<>(
			JavaConversions.seqAsJavaList(sink.getRetractResults()));
		Collections.sort(expected);
		Collections.sort(results);

		Assert.assertEquals(expected, results);
	}

}
