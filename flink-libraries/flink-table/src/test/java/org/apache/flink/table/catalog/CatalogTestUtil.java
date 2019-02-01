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
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;
import org.apache.flink.table.catalog.config.CatalogTableConfig;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import scala.Option;

import static org.apache.flink.table.catalog.CatalogTestBase.TEST_COMMENT;
import static org.junit.Assert.assertEquals;

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

	public static CatalogTable createCatalogTableWithPrimaryKey(boolean isStreaming) {
		return createCatalogTableWithPrimaryKey(getTestData(), isStreaming);
	}

	public static CatalogTable createCatalogTableWithPrimaryKey(List<Row> data, boolean isStreaming) {
	TableSchema tableSchema = TableSchemaUtil.fromDataType(new TypeInfoWrappedDataType(getRowTypeInfo()), Option.empty());

		RichTableSchema richTableSchema = new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
		richTableSchema.setPrimaryKey("a");

		CollectionTableFactory.initData(getRowTypeInfo(), data);

		return new CatalogTable(
			"COLLECTION",
			tableSchema,
			new HashMap<String, String>() {{
				put(CatalogTableConfig.IS_STREAMING, String.valueOf(isStreaming));
			}},
			richTableSchema,
			TableStats.builder().rowCount((long) data.size()).build(),
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

		return createCatalogTable(
			tableType,
			schema,
			TableStats.builder().rowCount(0L).build(),
			tableProperties,
			new LinkedHashSet<>());
	}

	public static CatalogTable createCatalogTable(
		String tableType,
		TableSchema schema,
		TableStats stats,
		Map<String, String> tableProperties,
		LinkedHashSet<String> partitionCols) {

		tableProperties.put(CatalogTableConfig.IS_STREAMING, "true");

		return new CatalogTable(
			tableType,
			schema,
			tableProperties,
			new RichTableSchema(schema.getFieldNames(), schema.getFieldTypes()),
			stats,
			TEST_COMMENT,
			partitionCols,
			partitionCols != null && !partitionCols.isEmpty(),
			null,
			null,
			-1L,
			0L,
			-1L);
	}

	public static void compare(CatalogTable t1, CatalogTable t2) {
		assertEquals(t1.getTableType(), t2.getTableType());
		assertEquals(t1.getTableSchema(), t2.getTableSchema());
		assertEquals(t1.getTableStats(), t2.getTableStats());
		assertEquals(t1.isPartitioned(), t2.isPartitioned());
		assertEquals(t1.getPartitionColumnNames(), t2.getPartitionColumnNames());
		assertEquals(t1.getComment(), t2.getComment());
		assertEquals(
			Boolean.valueOf(t1.getProperties().get(CatalogTableConfig.IS_STREAMING)),
			Boolean.valueOf(t2.getProperties().get(CatalogTableConfig.IS_STREAMING)));

	}

	protected static void compare(CatalogView v1, CatalogView v2) {
		compare(v1, v2);
		assertEquals(v1.getOriginalQuery(), v2.getOriginalQuery());
		assertEquals(v1.getExpandedQuery(), v2.getExpandedQuery());
	}

	protected static void compare(CatalogDatabase d1, CatalogDatabase d2) {
		assertEquals(d1.getProperties(), d2.getProperties());
	}

	protected static void compare(CatalogPartition p1, CatalogPartition p2) {
		assertEquals(p1.getPartitionSpec(), p2.getPartitionSpec());
	}

	protected static void compare(CatalogFunction f1, CatalogFunction f2) {
		assertEquals(f1.getSource(), f2.getSource());
		assertEquals(f1.getClazzName(), f2.getClazzName());
		assertEquals(f1.getProperties(), f2.getProperties());
	}
}
