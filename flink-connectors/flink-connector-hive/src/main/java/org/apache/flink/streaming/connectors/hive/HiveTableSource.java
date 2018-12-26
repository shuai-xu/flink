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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.util.TableSchemaUtil;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

/**
 * Hive table source class use GenericRow as inner implementation.
 */
public class HiveTableSource implements BatchTableSource<GenericRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableSource.class);
	RowTypeInfo rowTypeInfo;
	JobConf jobConf;
	TableStats tableStats;

	public HiveTableSource(RowTypeInfo rowTypeInfo, JobConf jobConf, TableStats tableStats) {
		this.rowTypeInfo = rowTypeInfo;
		this.jobConf = jobConf;
		this.tableStats = tableStats;
	}

	@Override
	public DataStream<GenericRow> getBoundedStream(StreamExecutionEnvironment streamEnv) {
		try {
			return streamEnv.createInput(new HiveTableInputFormat.Builder(rowTypeInfo, jobConf).build())
							.name(explainSource());
		} catch (Exception e){
			logger.error("Can not normally create hiveTableInputFormat !", e);
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataType getReturnType() {
		return DataTypes.internal(new BaseRowTypeInfo(
					GenericRow.class,
					rowTypeInfo.getFieldTypes(),
					rowTypeInfo.getFieldNames()));
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchemaUtil.fromDataType(getReturnType(), Option.empty());
	}

	@Override
	public String explainSource() {
		return "hive-table-source";
	}

	@Override
	public TableStats getTableStats() {
		return tableStats;
	}
}
