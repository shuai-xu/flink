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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import scala.Option;

/**
 * Test util for catalogs.
 */
public class CatalogTestUtil {
	public static List<Row> getTestData() {
		List<Row> data = new ArrayList<>();
		data.add(toRow(new Integer(1), new Integer(2)));
		data.add(toRow(new Integer(1), new Integer(3)));

		return data;
	}

	public static CatalogTable createCatalogTable() {
		return createCatalogTable(getTestData());
	}

	public static CatalogTable createCatalogTable(List<Row> data) {
		TableSchema tableSchema = TableSchemaUtil.fromDataType(DataTypes.of(getRowTypeInfo()), Option.empty());

		RichTableSchema richTableSchema = new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
		richTableSchema.setPrimaryKey("a");

		CollectionTableFactory.initData(getRowTypeInfo(), data);

		return new CatalogTable(
			"collection",
			tableSchema,
			new HashMap<>(),
			richTableSchema,
			new TableStats((long) data.size(), new HashMap<>()),
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			System.currentTimeMillis(),
			-1L
		);
	}

	public static RowTypeInfo getRowTypeInfo() {
		return new RowTypeInfo(
			new TypeInformation[] {BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO},
			new String[] {"a", "b"});
	}

	public static Row toRow(Object... args) {
		Row row = new Row(args.length);

		for (int i = 0; i < args.length; i++) {
			row.setField(i, args[i]);
		}

		return row;
	}

	public static CatalogTable createCatalogTable(
		String tableType,
		TableSchema schema,
		Map<String, String> tableProperties) {

		return new CatalogTable(
			tableType,
			schema,
			tableProperties,
			new RichTableSchema(schema.getFieldNames(), schema.getFieldTypes()),
			new TableStats(),
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			0L,
			-1L);
	}

	public static CatalogTable createCatalogTable(
		String tableType,
		TableSchema schema,
		Map<String, String> tableProperties,
		LinkedHashSet<String> partitionCols) {

		return new CatalogTable(
			tableType,
			schema,
			tableProperties,
			new RichTableSchema(schema.getFieldNames(), schema.getFieldTypes()),
			new TableStats(),
			null,
			partitionCols,
			true,
			null,
			null,
			-1L,
			0L,
			-1L);
	}

}
