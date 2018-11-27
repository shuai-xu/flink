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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest {
	private static final String TABLE_TYPE = "csv";

	private static final String DB1 = "db1";
	private static final String DB2 = "db2";
	private static final String TB1 = "tb1";
	private static final String TB2 = "tb2";
	private static final String TB3 = "tb3";

	private IMetaStoreClient mockedClient = Mockito.mock(IMetaStoreClient.class);
	private HiveCatalog hiveCatalog = new HiveCatalog("test", mockedClient);

	@Test
	public void testGetTable() throws TException {
		Mockito.when(mockedClient.getTable(anyString(), anyString())).thenReturn(getHiveTable());

		ExternalCatalogTable table = hiveCatalog.getTable(ObjectPath.fromString("default.hivetable"));

		assertEquals(TABLE_TYPE, table.tableType());
		assertEquals(hiveCatalog.getTableSchema(getFieldSchemas()), table.schema());
	}

	@Test
	public void testListTables() throws TException {
		Mockito.when(mockedClient.getAllDatabases()).thenReturn(Arrays.asList(DB1, DB2));
		Mockito.when(mockedClient.getAllTables(DB1)).thenReturn(Arrays.asList(TB1));
		Mockito.when(mockedClient.getAllTables(DB2)).thenReturn(Arrays.asList(TB2, TB3));

		assertEquals(Arrays.asList(
				new ObjectPath(DB1, TB1),
				new ObjectPath(DB2, TB2),
				new ObjectPath(DB2, TB3)
			),
			hiveCatalog.listAllTables());
	}

	private Table getHiveTable() {
		return new Table(
			"hive_table",
			"default",
			"fake_owner",
			-1,
			-1,
			-1,
			getStorageDescriptor(),
			Collections.emptyList(),
			new HashMap<String, String>() {{
				put("k1", "v1");
				put("k2", "v2");
			}},
			null,
			null,
			TABLE_TYPE
		);
	}

	private StorageDescriptor getStorageDescriptor() {
		return new StorageDescriptor(
			getFieldSchemas(),
			"mylocation",
			null,
			null,
			false,
			1,
			null,
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyMap()
		);
	}

	private List<FieldSchema> getFieldSchemas() {
		return Arrays.asList(
			new FieldSchema("a", serdeConstants.STRING_TYPE_NAME, null),
			new FieldSchema("b", serdeConstants.INT_TYPE_NAME, null)
		);
	}
}
