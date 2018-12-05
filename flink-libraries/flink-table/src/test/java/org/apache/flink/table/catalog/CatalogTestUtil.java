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
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import scala.Option;

/**
 * Test util for catalogs.
 */
public class CatalogTestUtil {
	public static ExternalCatalogTable getTestExternalCatalogTable() {

		List<Row> data = new LinkedList<>();
		data.add(toRow(new Integer(1), new Integer(2)));
		data.add(toRow(new Integer(1), new Integer(3)));

		TableSchema tableSchema = TableSchema.fromDataType(DataTypes.of(getRowTypeInfo()), Option.empty());

		RichTableSchema richTableSchema = new RichTableSchema(tableSchema.getColumnNames(), tableSchema.getTypes());
		richTableSchema.setPrimaryKey("a");

		CollectionTableFactory.initData(getRowTypeInfo(), data);

		return new ExternalCatalogTable(
			"collections",
			tableSchema,
			new HashMap<>(),
			richTableSchema,
			null,
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

	private static Row toRow(Object... args) {
		Row row = new Row(args.length);

		for (int i = 0; i < args.length; i++) {
			row.setField(i, args[i]);
		}

		return row;
	}
}
