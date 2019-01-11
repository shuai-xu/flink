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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.connectors.hbase.HTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupConfig;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * HBaseTableSource for hbase 1.4.3.
 */
public class HBase143TableSource implements StreamTableSource<Row>, LookupableTableSource<Row>, BatchTableSource<Row> {
	RichTableSchema sqlSchema;
	HTableSchema hbaseSchema;
	int rowKeyIndex;
	List<Integer> qualifierIndexes;
	Map<String, String> userParams;

	public HBase143TableSource(
			RichTableSchema sqlSchema,
			HTableSchema hbaseSchema,
			int rowKeyIndex,
			List<Integer> qualifierIndexes,
			Map<String, String> userParams) {
		Preconditions.checkArgument(null != sqlSchema && null != hbaseSchema, "given sql and hbase schemas should not be null!");
		// only support single sql primary key mapping to hbase rowKey for now.
		Preconditions.checkArgument(1 == sqlSchema.getPrimaryKeys().size(),
			"A single primary key sql schema is necessary for the HBaseBridgeTable!");
		int hbaseQualifierCnt = hbaseSchema.getFlatQualifiers().size();
		int sqlColumnCnt = sqlSchema.getColumnNames().length;
		Preconditions.checkArgument(hbaseQualifierCnt == sqlColumnCnt,
			"the given hbase schema's qualifier number(" + hbaseQualifierCnt +
			") is not consist with sql schema's column number(" + sqlColumnCnt + ")!");
		this.sqlSchema = sqlSchema;
		this.hbaseSchema = hbaseSchema;
		this.rowKeyIndex = rowKeyIndex;
		this.qualifierIndexes = qualifierIndexes;
		this.userParams = userParams;
	}

	@Override
	public TableFunction<Row> getLookupFunction(int[] lookupKeys) {
		Preconditions.checkArgument(null != lookupKeys && lookupKeys.length == 1, "HBase table can only be retrieved by rowKey for now.");
		try {
			return new HBase143LookupFunction(hbaseSchema, rowKeyIndex, qualifierIndexes);
		} catch (IOException e) {
			throw new RuntimeException("encounter an IOException when initialize the HBase143LookupFunction.", e);
		}
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(int[] lookupKeys) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public LookupConfig getLookupConfig() {
		// use default value for now.
		return new LookupConfig();
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to DataStream currently.");
	}

	@Override
	public DataStream<Row> getBoundedStream(StreamExecutionEnvironment streamEnv) {
		throw new UnsupportedOperationException("HBase table can not convert to BoundedDataStream currently.");
	}

	@Override
	public DataType getReturnType() {
		return  DataTypes.createRowType(sqlSchema.getColumnTypes(), sqlSchema.getColumnNames());
	}

	@Override
	public TableSchema getTableSchema() {
		String rowKeyColumn = sqlSchema.getColumnNames()[rowKeyIndex];
		return TableSchemaUtil.builderFromDataType(getReturnType()).primaryKey(rowKeyColumn).build();
	}

	@Override
	public String explainSource() {
		return "HBase[" + hbaseSchema.getTableName() + "], schema:{" + getReturnType() + "}";
	}

	@Override
	public TableStats getTableStats() {
		return null;
	}
}
