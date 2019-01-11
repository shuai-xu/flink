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

package org.apache.flink.connectors.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.HTableSchema;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The HTableSchemaUtil helps to construct HTableSchema and column index mapping info from RichTableSchema and properties.
 */
@Internal
public class HTableSchemaUtil {

	/**
	 * Create HTableSchema from an existed RichTableSchema and given `singleColumnFamily` name.
	 * Sql column name in the table schema will be the HBase qualifier name.
	 * Returns a Tuple3 result: (hTableSchema, pkIndex, qualifierColumnIndex)
	 */
	public static Tuple3<HTableSchema, Integer, List<Integer>> fromRichTableSchemaWithSingleColumnFamily(
			String tableName,
			RichTableSchema tableSchema,
			String singleColumnFamily) {
		preCheck(tableName, tableSchema);
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(singleColumnFamily), "column Mapping should not be empty.");

		String pkColumn = tableSchema.getPrimaryKeys().get(0);
		HTableSchema.HColumnBuilder cfBuilder = new HTableSchema.HColumnBuilder(singleColumnFamily);
		int pkIndex = -1;
		TypeInformation pkType = null;
		List<Integer> qualifierColumnIndex = new ArrayList<>();
		for (int idx = 0; idx < tableSchema.getColumnNames().length; idx++) {
			TypeInformation type = DataTypes.toTypeInfo(tableSchema.getColumnTypes()[idx]);
			String sqlColumnName = tableSchema.getColumnNames()[idx];
			if (!pkColumn.equals(sqlColumnName)) {
				cfBuilder.addQualifier(new HTableSchema.HQualifier(sqlColumnName, type));
				qualifierColumnIndex.add(idx);
			} else {
				pkIndex = idx;
				pkType = DataTypes.toTypeInfo(tableSchema.getColumnTypes()[idx]);
			}
		}
		HTableSchema.RowKey rowKey = new HTableSchema.RowKey(pkColumn, pkType);
		HTableSchema hTableSchema = new HTableSchema.Builder(tableName, rowKey).addHColumn(cfBuilder.build()).build();
		return new Tuple3<>(hTableSchema, pkIndex, qualifierColumnIndex);
	}

	/**
	 * Create HTableSchema from an existed RichTableSchema and given `columnMapping` param.
	 * e.g., 'a:name as name, a:age as age'
	 * will mapping to the table schema (name, age) and convert to HBase qualifiers: 'a:name' and 'a:age'
	 */
	public static Tuple3<HTableSchema, Integer, List<Integer>> fromRichTableSchemaWithColumnMapping(
			String tableName,
			RichTableSchema tableSchema,
			String columnMapping) {
		// TODO support mutli columnfamily by columnMapping
		/*preCheck(tableName, tableSchema);
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(columnMapping), "column Mapping should not be empty.");

		String pkColumn = tableSchema.getPrimaryKeys().get(0);
		Map<String, Tuple3<String, String, String>> mapping = getColumnMapping(columnMapping);

		HTableSchema.HColumnBuilder cfBuilder = new HTableSchema.HColumnBuilder();
		TypeInformation pkType = null;
		for (int idx = 0; idx < tableSchema.getColumnNames().length; idx++) {
			TypeInformation type =  DataTypes.toTypeInfo(tableSchema.getColumnTypes()[idx]);
			String sqlColumnName = tableSchema.getColumnNames()[idx];
			if (!pkColumn.equals(sqlColumnName)) {
				Tuple3<String, String, String> qInfo = mapping.get(sqlColumnName);
				if (null == qInfo) {
					throw new RuntimeException("missing column mapping for column: " + sqlColumnName);
				}
//				(qInfo.f0, qInfo.f1, qInfo.f2, type);
				cfBuilder.addQualifier(new HTableSchema.HQualifier(sqlColumnName, type));
			} else {
				pkType = DataTypes.toTypeInfo(tableSchema.getColumnTypes()[idx]);
			}
		}
		HTableSchema.RowKey rowKey = new HTableSchema.RowKey(pkColumn, pkType);
		return new HTableSchema.Builder(tableName, rowKey).addHColumn(cfBuilder.build()).build();*/
		return null;
	}

	static void preCheck(String sqlTableName, RichTableSchema tableSchema) {
		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(sqlTableName), "sql table name should not be empty.");
		Preconditions.checkNotNull(tableSchema, "table schema should not be null.");
		// hbase table should and only have a single column primary key for rowkey
		Preconditions.checkArgument(null != tableSchema.getPrimaryKeys() && 1 == tableSchema.getPrimaryKeys().size(), "column Mapping should not be empty.");
	}

	static Map<String, Tuple3<String, String, String>> getColumnMapping(String columnMapping) {
		Map<String, Tuple3<String, String, String>> map = new HashMap<>();
		String[] seps = columnMapping.split(",");
		for (int idx = 0; idx < seps.length; idx++) {
			String sep = seps[idx];
			if (!StringUtils.isNullOrWhitespaceOnly(sep)) {
				sep = sep.trim();
				String[] kv = sep.split("\\s+");
				if (kv == null || kv.length != 3) {
					throw new RuntimeException("One columnMapping should like cf:qualifier as qualifier");
				}
				if (StringUtils.isNullOrWhitespaceOnly(kv[2]) || StringUtils.isNullOrWhitespaceOnly(kv[0]) || !"AS".equalsIgnoreCase(kv[1])) {
					throw new RuntimeException("Invalid columnMapping " + sep);
				}
				String[] keys = kv[0].split(":");
				if (keys.length != 2 || StringUtils.isNullOrWhitespaceOnly(keys[0])) {
					throw new RuntimeException("Invalid columnMapping " + sep);
				}
				Tuple3<String, String, String> q = new Tuple3<>(keys[0], keys[1], kv[2]);
				map.put(kv[2], q);
			}
		}
		return map;
	}
}
