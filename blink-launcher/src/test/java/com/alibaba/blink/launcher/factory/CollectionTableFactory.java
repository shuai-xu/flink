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

package com.alibaba.blink.launcher.factory;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.factories.FlinkTableFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchExecTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.AbstractTableSource;
import org.apache.flink.table.sources.BatchExecTableSource;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import scala.Option;

/**
 * Factory of collection table.
 */
public class CollectionTableFactory implements TableFactory {

	public static final List DATA = new LinkedList<>();

	public static final List<Row> RESULT = new LinkedList<>();

	public static TypeInformation rowType = null;

	public static TableSourceParser parser = null;

	public static RowTypeInfo sinkType = null;

	public static long emitIntervalMs = 1000;

	static {
		FlinkTableFactory.DIRECTORY
			.put("COLLECTION", "com.alibaba.blink.launcher.factory.CollectionTableFactory");
	}

	public static void initData(RowTypeInfo rowTypeInfo, Collection<Row> data) {
		CollectionTableFactory.RESULT.clear();
		CollectionTableFactory.DATA.clear();
		CollectionTableFactory.parser = null;
		CollectionTableFactory.sinkType = rowTypeInfo;
		CollectionTableFactory.rowType = rowTypeInfo;
		CollectionTableFactory.DATA.addAll(data);
	}

	public static <T> void initData(
		TypeInformation<T> rowType, TableSourceParser parser, RowTypeInfo sinkType,
		Collection<T> data) {

		CollectionTableFactory.RESULT.clear();
		CollectionTableFactory.DATA.clear();
		CollectionTableFactory.sinkType = sinkType;
		CollectionTableFactory.rowType = rowType;
		CollectionTableFactory.DATA.addAll(data);
		CollectionTableFactory.parser = parser;
		CollectionTableFactory.emitIntervalMs = 0;
	}

	public static void initData(
		RowTypeInfo rowTypeInfo, RowTypeInfo sinkType, Collection<Row> data) {
		CollectionTableFactory.RESULT.clear();
		CollectionTableFactory.DATA.clear();
		CollectionTableFactory.parser = null;
		CollectionTableFactory.sinkType = sinkType;
		CollectionTableFactory.rowType = rowTypeInfo;
		CollectionTableFactory.DATA.addAll(data);
		CollectionTableFactory.emitIntervalMs = 0;
	}

	@Override
	public TableSource createTableSource(
		String tableName, RichTableSchema schema, TableProperties properties) {
		return new CollectionTableSource(tableName);
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(
		String tableName, RichTableSchema schema, TableProperties properties) {
		throw new RuntimeException("Dimension table not supported");
	}

	@Override
	public TableSink<?> createTableSink(
		String tableName, RichTableSchema schema, TableProperties properties) {
		return new CollectionTableSink(tableName);
	}

	@Override
	public TableSourceParser createParser(
		String tableName, RichTableSchema schema, TableProperties properties) {
		return parser;
	}

	/**
	 * Collection inputFormat for testing.
	 */
	public static class TestCollectionInputFormat<T> extends CollectionInputFormat<T> {

		public TestCollectionInputFormat(Collection<T> dataSet, TypeSerializer<T> serializer) {
			super(dataSet, serializer);
		}

		public boolean reachedEnd() throws IOException {
			try {
				Thread.currentThread().sleep(emitIntervalMs);
			} catch (InterruptedException e) {
			}
			return super.reachedEnd();
		}
	}

	/**
	 * Table source of collection.
	 */
	public static class CollectionTableSource<T>
		extends AbstractTableSource
		implements BatchExecTableSource<T>, StreamTableSource<T> {

		private String name;

		public CollectionTableSource(String name) {
			this.name = name;
		}

		@Override
		public DataStream<T> getBoundedStream(StreamExecutionEnvironment streamEnv) {
			return streamEnv.createInput(
				new TestCollectionInputFormat<>(DATA,
					rowType.createSerializer(new ExecutionConfig())),
				rowType, name);
		}

		@Override
		public DataType getReturnType() {
			return DataTypes.of(rowType);
		}

		@Override
		public TableSchema getTableSchema() {
			return TableSchema.fromDataType(getReturnType(), Option.empty());
		}

		@Override
		public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv) {
			return new DataStream<>(execEnv, getBoundedStream(execEnv).getTransformation());
		}

		@Override
		public String explainSource() {
			return "";
		}
	}

	/**
	 * Sink function of unsafe memory.
	 */
	public static class UnsafeMemorySinkFunction extends RichSinkFunction<Row> {

		@Override
		public void open(Configuration param) {
			RESULT.clear();
		}

		@Override
		public void invoke(Row row) throws Exception {
			RESULT.add(row);
		}
	}

	/**
	 * Table sink of collection.
	 */
	public static class CollectionTableSink
		implements BatchExecTableSink<Row>, AppendStreamTableSink<Row> {

		private String name;

		public CollectionTableSink(String name) {
			this.name = name;
		}

		@Override
		public DataStreamSink<Row> emitBoundedStream(
			DataStream<Row> boundedStream,
			TableConfig tableConfig, ExecutionConfig executionConfig) {
			DataStreamSink<Row> bounded = boundedStream.addSink(new UnsafeMemorySinkFunction())
				.name(name)
				.setParallelism(1);
			bounded.getTransformation().setParallelismLocked(true);
			return bounded;
		}

		@Override
		public DataType getOutputType() {
			return DataTypes.of(sinkType);
		}

		@Override
		public String[] getFieldNames() {
			return sinkType.getFieldNames();
		}

		@Override
		public DataType[] getFieldTypes() {
			return DataTypes.dataTypes(sinkType.getFieldTypes());
		}

		@Override
		public TableSink<Row> configure(String[] fieldNames, DataType[] fieldTypes) {
			return this;
		}

		@Override
		public void emitDataStream(DataStream<Row> dataStream) {
			dataStream.addSink(new UnsafeMemorySinkFunction()).setParallelism(1);
		}
	}
}
