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

package org.apache.flink.connectors.hbase.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * This is an simple example how to write streams into a HBase table.
 */
public class HBaseWriterExample {
	static String family = "cf";
	static String qualify1 = "q1";
	static String qualify2 = "q2";

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// prepare a stream source with random numbers
		DataStream<String> dataStream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 1L;

			private volatile boolean isRunning = true;

			@Override
			public void run(SourceContext<String> out) throws Exception {
				while (isRunning) {
					out.collect(String.valueOf(Math.floor(Math.random() * 100)));
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		});

		// prepare sink table info for SinkFunction
		HBaseTableSchemaV2 hBaseTableSchema = new HBaseTableSchemaV2.Builder("ROWKEY", BasicTypeInfo.LONG_TYPE_INFO)
			.addColumn(family, qualify1, BasicTypeInfo.INT_TYPE_INFO)
			.addColumn(family, qualify2, BasicTypeInfo.STRING_TYPE_INFO)
			.build();
		// init a default hbase configuration from current classpath.
		// and you can explicitly set the `hbase.zookeeper.quorum` to access remote hbase cluster.
		// conf.set("hbase.zookeeper.quorum", "...");
		Configuration conf = HBaseConfiguration.create();

		// process source data, here we convert it to a Tuple2 structure
		dataStream.flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
				if (StringUtils.isNullOrWhitespaceOnly(value)) {
					out.collect(new Tuple2<>(0, ""));
				} else {
					out.collect(new Tuple2<>(value.length(), value));
				}
			}
		}).addSink(
		// write to sink using a sink function and
			new HBaseSimpleWriter("example_table", hBaseTableSchema, conf));

		// execute
		env.execute();
	}

	/**
	 * A simple hbase writer subclass which accept Tuple2 input type and do put operation only.
	 */
	private static class HBaseSimpleWriter extends HBaseWriterBase<Tuple2<Integer, String>> {
		static byte[] cf = Bytes.toBytes(family);
		static byte[] q1 = Bytes.toBytes(qualify1);
		static byte[] q2 = Bytes.toBytes(qualify2);

		public HBaseSimpleWriter(
				String hTableName,
				HBaseTableSchemaV2 hTableSchema,
				Configuration conf) throws IOException {
			super(hTableName, hTableSchema, conf);
		}

		@Override
		public void invoke(Tuple2<Integer, String> value, Context context) throws Exception {
			// perform a simple serialization here, you can do more via hbase table schema
			// e.g., `hTableSchema.getFamilySchema().getFlatStringQualifiers()` returns all qualifiers' names and types info.
			long sysTime = System.currentTimeMillis();
			Put put = new Put(Bytes.toBytes(sysTime));
			put.addColumn(cf, q1, Bytes.toBytes(value.f0));
			put.addColumn(cf, q2, Bytes.toBytes(value.f1));
			table.put(put);
		}
	}
}
