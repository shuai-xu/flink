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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;

import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * Base for unit tests of a specific catalog, like FlinkInMemoryCatalog and HiveCatalog.
 */
public class CatalogTestBase {
	protected CatalogDatabase createDb() {
		return new CatalogDatabase(new HashMap<>());
	}

	protected CatalogDatabase createAnotherDb() {
		return new CatalogDatabase(new HashMap<String, String>() {{
			put("key", "value");
		}});
	}

	protected ExternalCatalogTable createTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.INT
			}
		);

		return createTable(schema);
	}

	protected ExternalCatalogTable createAnotherTable() {
		TableSchema schema = new TableSchema(
			new String[] {"first", "second"},
			new InternalType[]{
				DataTypes.STRING,
				DataTypes.STRING  // different from create table instance.
			}
		);

		return createTable(schema);
	}

	private ExternalCatalogTable createTable(TableSchema schema) {
		return new ExternalCatalogTable(
			"csv",
			schema,
			new HashMap<String, String>(),
			null,
			null,
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			0L,
			-1L);
	}
}
