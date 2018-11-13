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

package com.alibaba.blink.launcher.autoconfig.connector;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.api.types.BaseRowType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Source for testing.
 */
public class DummyTableFactory implements TableFactory {
	public DummyTableFactory() {
	}

	@Override
	public TableSourceParser createParser(String tableName, RichTableSchema tableSchema, TableProperties properties) {
		DummyParser parser = new DummyParser(tableSchema, properties);
		return new TableSourceParser(
			new SourceCollectorTableFunction<>(parser),
			Collections.singletonList("f0"));
	}

	@Override
	public TableSource createTableSource(String tableName, RichTableSchema richTableSchema, TableProperties tableProperties) {
		return new DummyTableSource(tableName);
	}

	@Override
	public DimensionTableSource<?> createDimensionTableSource(String tableName, RichTableSchema richTableSchema, TableProperties tableProperties) {
		return null;
	}

	@Override
	public TableSink<?> createTableSink(String tableName, RichTableSchema richTableSchema, TableProperties tableProperties) {
		throw new UnsupportedOperationException("DummyTable cannot be a sink.");
	}

	/**
	 * TableSource.
	 */
	public static class DummyTableSource extends SourceFunctionTableSource<List<String>> {
		private String tableName;

		public DummyTableSource(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public SourceFunction<List<String>> getSourceFunction() {
			return new DummySource(tableName);
		}

		@Override
		public String explainSource() {
			return String.format("DummySource-%s-Stream", tableName);
		}
	}

	/**
	 * SourceFunction.
	 */
	public static class DummySource extends AbstractParallelSource<List<String>, Long> {
		private String tableName;

		public DummySource() {
			this("");
		}

		public DummySource(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public List<String> getPartitionList() throws Exception {
			if (tableName.equals("custom_input1")) {
				return getPartitionList(16);
			} else if (tableName.equals("custom_input2")) {
				return getPartitionList(65);
			} else {
				return getPartitionList(32);
			}
		}

		@Override
		public ResourceSpec estimateSinglePartitionResource() {
			return ResourceSpec.newBuilder().setCpuCores(0.1).setHeapMemoryInMB(32).build();
		}

		private List<String> getPartitionList(int num) {
			List<String> list = new ArrayList<>();
			for (int i = 0; i < num; i++) {
				list.add("partition" + i);
			}
			return list;
		}
	}

	/**
	 * Parser.
	 */
	public static class DummyParser implements SourceCollector<List<String>, BaseRow> {
		protected BaseRowType rowType;

		public DummyParser(RichTableSchema schema, TableProperties params) {
			this(schema.getResultType(GenericRow.class), schema.getHeaderFields(), params);
		}

		public DummyParser(BaseRowType rowType, List<String> headerFields, TableProperties params) {
			this.rowType = rowType;
		}

		@Override
		public void parseAndCollect(List<String> strings, Collector<BaseRow> collector) {

		}

		@Override
		public TypeInformation<BaseRow> getProducedType() {
			return DataTypes.toTypeInfo(rowType);
		}
	}
}
