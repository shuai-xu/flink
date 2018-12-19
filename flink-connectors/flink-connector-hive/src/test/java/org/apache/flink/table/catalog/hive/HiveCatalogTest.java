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

import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.config.HiveTableConfig;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest extends CatalogTestBase {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static String warehouseDir;
	private static String warehouseUri;

	@BeforeClass
	public static void init() throws IOException {
		warehouseDir = TEMPORARY_FOLDER.newFolder().getAbsolutePath() + "/metastore_db";
		warehouseUri = String.format("jdbc:derby:;databaseName=%s;create=true", warehouseDir);
		HiveConf hiveConf = new HiveConf();
		hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
		hiveConf.setBoolean("datanucleus.schema.autoCreateTables", true);
		hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEMPORARY_FOLDER.newFolder("hive_warehouse").getAbsolutePath());
		hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, warehouseUri);

		catalog = new HiveCatalog("test", hiveConf);
		catalog.open();
	}

	@After
	public void close() {
		catalog.dropTable(new ObjectPath(db1, t1), true);
		catalog.dropTable(new ObjectPath(db2, t2), true);
		catalog.dropDatabase(db1, true);
		catalog.dropDatabase(db2, true);
	}

	@AfterClass
	public static void clean() throws IOException {
		catalog.close();
	}

	@Override
	public String getTableType() {
		return "hive";
	}

	@Override
	protected Map<String, String> getTableProperties() {
		return new HashMap<String, String>() {{
			put(HiveTableConfig.HIVE_TABLE_LOCATION, warehouseDir + "/tmp");
		}};
	}
}
