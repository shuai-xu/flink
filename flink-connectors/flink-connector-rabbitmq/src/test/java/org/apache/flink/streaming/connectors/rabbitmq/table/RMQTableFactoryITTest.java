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

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.InternalType;

import com.alibaba.blink.table.api.RichTableSchema;
import com.alibaba.blink.table.api.TableProperties;
import org.junit.Ignore;
import org.junit.Test;

/**
 * RMQ table factory IT tests.
 */
@Ignore
public class RMQTableFactoryITTest {
	@Test
	public void testTableSink() throws Exception {
		TableProperties properties = new TableProperties();
		properties.setString("queueField", "queue");
		properties.setString("msgField", "msg");
		properties.setString("uri", "amqp://localhost:5672");
		RichTableSchema schema = new RichTableSchema(
				new String[]{"queue", "msg"},
				new InternalType[]{DataTypes.STRING, DataTypes.STRING});

		RMQTableFactory tableFactory = new RMQTableFactory();
		TableSink sink = tableFactory.createTableSink("test", schema, properties);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getBatchTableEnvironment(env);
		tableEnv.registerTableSink("test_sink", sink.getFieldNames(), sink.getFieldTypes(), sink);
		DataStream boundedStream =
				env.fromElements(
						Tuple2.of("1", "2"), Tuple2.of("2", "4"));
		tableEnv.registerBoundedStreamInternal("test_source", boundedStream);
		tableEnv.sqlUpdate("insert into test_sink select * from test_source");
		tableEnv.execute();
	}

}
