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

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.sinks.BatchCompatibleStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Rabbit MQ table sink which publishes message directly to a queue.
 */
public class RMQTableSink implements UpsertStreamTableSink<Row>,
		BatchCompatibleStreamTableSink<Tuple2<Boolean, Row>>, Serializable {

	private RMQConnectionConfig connectionConfig;

	private String[] fieldNames;
	private InternalType[] fieldTypes;
	private int queueNameIndex = -1;
	private int msgFieldIndex = -1;

	public RMQTableSink(
			String queueNameField,
			String msgField,
			RMQConnectionConfig connectionConfig,
			RichTableSchema schema) {
		this.connectionConfig = connectionConfig;
		this.fieldNames = schema.getColumnNames();
		this.fieldTypes = schema.getColumnTypes();
		for (int i = 0; i < fieldNames.length; i++) {
			if (queueNameField.equals(fieldNames[i])) {
				queueNameIndex = i;
			}
			if (msgField.equals(fieldNames[i])) {
				msgFieldIndex = i;
			}
		}
		Preconditions.checkArgument(
				queueNameIndex >= 0, "Queue name field " + queueNameField + " not existed in schema.");
		Preconditions.checkArgument(
				msgFieldIndex >= 0, "Message field " + msgField + " not existed in schema.");
	}

	@Override
	public void setKeyFields(String[] keys) {
		// do nothing
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// do nothing
	}

	@Override
	public DataType getRecordType() {
		return DataTypes.createRowType(fieldTypes, fieldNames);
	}

	@Override
	public DataStreamSink<?> emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		return emitBoundedStream(dataStream);
	}

	@Override
	public DataStreamSink<?> emitBoundedStream(DataStream<Tuple2<Boolean, Row>> boundedStream) {
		return boundedStream.flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Row> () {
			@Override
			public void flatMap(Tuple2<Boolean, Row> value, Collector<Row> out) throws Exception {
				if (value.f0) {
					out.collect(value.f1);
				}
			}
		}).returns(DataTypes.toTypeInfo(getRecordType()))
				.setParallelism(boundedStream.getParallelism())
				.addSink(
						new RMQSink(
								connectionConfig,
								new StringSerializationSchema(msgFieldIndex),
								new DirectPublishOptions(queueNameIndex)))
				.setParallelism(boundedStream.getParallelism());
	}

	@Override
	public DataType getOutputType() {
		return DataTypes.createTupleType(DataTypes.BOOLEAN, getRecordType());
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public DataType[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames,
			DataType[] fieldTypes) {
		// Has been initialized in constructor, do nothing
		return this;
	}

}
