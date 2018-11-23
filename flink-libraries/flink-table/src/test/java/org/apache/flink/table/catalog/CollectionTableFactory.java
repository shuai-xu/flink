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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.factories.FlinkTableFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.AbstractTableSource;
import org.apache.flink.table.sources.AsyncConfig;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.IndexKey;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import scala.Option;

/**
 * Factory of collection table.
 */
public class CollectionTableFactory implements TableFactory {

	public static final List<Row> DATA = new LinkedList<>();

	public static final List<Row> RESULT = new LinkedList<>();

	public static TypeInformation rowType = null;

	public static TableSourceParser parser = null;

	public static RowTypeInfo sinkType = null;

	public static long emitIntervalMs = 1000;

	public static boolean checkParam = false;

	public static final String TABLE_TYPE_KEY = "tabletype";
	public static final int SOURCE = 1;
	public static final int DIM = 2;
	public static final int SINK = 3;

	static {
		FlinkTableFactory.DIRECTORY
			.put("COLLECTION", CollectionTableFactory.class.getCanonicalName());
	}

	public static void initData(RowTypeInfo rowTypeInfo, Collection<Row> data) {
		CollectionTableFactory.RESULT.clear();
		CollectionTableFactory.DATA.clear();
		CollectionTableFactory.parser = null;
		CollectionTableFactory.sinkType = rowTypeInfo;
		CollectionTableFactory.rowType = rowTypeInfo;
		CollectionTableFactory.DATA.addAll(data);
	}

	@Override
	public TableSource createTableSource(
		String tableName, RichTableSchema schema, TableProperties properties) {
		if (checkParam) {
			Preconditions.checkArgument(properties.getInteger(TABLE_TYPE_KEY, -1) == SOURCE);
		}
		return new CollectionTableSource(tableName);
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(
		String tableName, RichTableSchema schema, TableProperties properties) {
		if (checkParam) {
			Preconditions.checkArgument(properties.getInteger(TABLE_TYPE_KEY, -1) == DIM);
		}
		return new CollectionDimensionTable(schema);
	}

	@Override
	public TableSink<?> createTableSink(
		String tableName, RichTableSchema schema, TableProperties properties) {
		if (checkParam) {
			Preconditions.checkArgument(properties.getInteger(TABLE_TYPE_KEY, -1) == SINK);
		}
		return new CollectionTableSink(tableName);
	}

	@Override
	public TableSourceParser createParser(
		String tableName, RichTableSchema schema, TableProperties properties) {
		return parser;
	}

	/**
	 * Dimension table source fetcher.
	 */
	public static class DimFetcher implements FlatMapFunction<BaseRow, BaseRow>, Serializable {

		private IndexKey keys;

		public DimFetcher(IndexKey keys) {
			this.keys = keys;
		}

		@Override
		public void flatMap(BaseRow value, Collector<BaseRow> out) throws Exception {
			int[] fieldMapping = keys.toArray();
			for (Row data : DATA) {
				boolean matched = true;
				for (int i = 0; i < fieldMapping.length; i++) {
					Object dataField = data.getField(fieldMapping[i]);
					Object inputField = null;
					if (dataField instanceof String) {
						inputField = value.getBinaryString(i).toString();
					} else if (dataField instanceof Integer) {
						inputField = Integer.valueOf(value.getInt(i));
					}
					if (!dataField.equals(inputField)) {
						matched = false;
						break;
					}
				}
				if (matched) {
					GenericRow row = new GenericRow(data.getArity());
					for (int i = 0; i < data.getArity(); i++) {
						Object dataField = data.getField(i);
						if (dataField instanceof String) {
							row.update(i, BinaryString.fromString(dataField));
						} else {
							row.update(i, dataField);
						}

					}
					out.collect(row);
				}
			}
		}
	}

	/**
	 * Dimension table source.
	 */
	public static class CollectionDimensionTable implements DimensionTableSource<BaseRow> {
		private RichTableSchema schema;

		public CollectionDimensionTable(RichTableSchema schema) {
			this.schema = schema;
		}

		@Override
		public Collection<IndexKey> getIndexes() {
			return schema.toIndexKeys();
		}

		@Override
		public FlatMapFunction<BaseRow, BaseRow> getLookupFunction(IndexKey keys) {
			return new DimFetcher(keys);
		}

		@Override
		public AsyncFunction<BaseRow, BaseRow> getAsyncLookupFunction(IndexKey keys) {
			return null;
		}

		@Override
		public boolean isTemporal() {
			return true;
		}

		@Override
		public boolean isAsync() {
			return false;
		}

		@Override
		public AsyncConfig getAsyncConfig() {
			return null;
		}

		@Override
		public DataType getReturnType() {
			return DataTypes.internal(rowType);
		}

		@Override
		public TableSchema getTableSchema() {
			return TableSchema.fromDataType(getReturnType(), Option.empty());
		}

		@Override
		public TableStats getTableStats() {
			return null;
		}

		@Override
		public String explainSource() {
			return "Test";
		}
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
		implements BatchTableSource<T>, StreamTableSource<T> {

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
	}

	/**
	 * Sink function of unsafe memory.
	 */
	public static class UnsafeMemorySinkFunction extends RichSinkFunction<Row> {

		private TypeSerializer<Row> serializer;

		@Override
		public void open(Configuration param) {
			RESULT.clear();
			serializer = rowType.createSerializer(new ExecutionConfig());
		}

		@Override
		public void invoke(Row row) throws Exception {
			RESULT.add(serializer.copy(row));
		}
	}

	/**
	 * Table sink of collection.
	 */
	public static class CollectionTableSink
		implements BatchTableSink<Row>, AppendStreamTableSink<Row> {

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
		public DataStreamSink<Row> emitDataStream(DataStream<Row> dataStream) {
			return dataStream.addSink(new UnsafeMemorySinkFunction()).setParallelism(1);
		}
	}
}
