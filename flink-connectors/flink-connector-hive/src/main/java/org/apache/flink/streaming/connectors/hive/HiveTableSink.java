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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.TypeConverters;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Hive table sink class.
 * Todo: complete write to partition table function.
 */
public class HiveTableSink implements BatchTableSink<BaseRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableSink.class);
	private final JobConf jobConf;
	private final RowTypeInfo rowTypeInfo;
	private final String dbName;
	private final String tableName;
	private final String[] partitionCols;
	private final Map<String, String> partitionValues;

	public HiveTableSink(
			JobConf jobConf,
			RowTypeInfo rowTypeInfo,
			String dbName,
			String tableName,
			String[] partitionCols,
			Map<String, String> partitionValues) {
		this.jobConf = jobConf;
		this.rowTypeInfo = rowTypeInfo;
		this.dbName = dbName;
		this.tableName = tableName;
		this.partitionCols = partitionCols;
		this.partitionValues = partitionValues;
	}

	@Override
	public DataStreamSink<?> emitBoundedStream(
			DataStream<BaseRow> boundedStream, TableConfig tableConfig, ExecutionConfig executionConfig) {
		StorageDescriptor sd = HiveTableUtil.createStorageDescriptor(jobConf, rowTypeInfo);
		HiveTablePartition hiveTablePartition = new HiveTablePartition(sd, null);
		HiveTableOutputFormat hiveTableOutputFormat = new HiveTableOutputFormat(jobConf, false, partitionCols,
																				rowTypeInfo, hiveTablePartition);
		return boundedStream.writeUsingOutputFormat(hiveTableOutputFormat);
	}

	@Override
	public DataType getOutputType() {
		return TypeConverters.createInternalTypeFromTypeInfo(new BaseRowTypeInfo(rowTypeInfo.getFieldTypes(),
																				rowTypeInfo.getFieldNames()));
	}

	@Override
	public String[] getFieldNames() {
		return rowTypeInfo.getFieldNames();
	}

	@Override
	public DataType[] getFieldTypes() {
		DataType[] dataTypes = new DataType[rowTypeInfo.getArity()];
		for (int i = 0; i < dataTypes.length; i++) {
			dataTypes[i] = TypeConverters.createInternalTypeFromTypeInfo(rowTypeInfo.getTypeAt(i));
		}
		return dataTypes;
	}

	@Override
	public TableSink<BaseRow> configure(
			String[] fieldNames, DataType[] fieldTypes) {
		return new HiveTableSink(jobConf, rowTypeInfo, dbName, tableName, partitionCols, partitionValues);
	}
}
