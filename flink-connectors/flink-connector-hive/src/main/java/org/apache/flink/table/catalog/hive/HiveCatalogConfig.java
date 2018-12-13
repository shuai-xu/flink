/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.hive;

/**
 * HiveCatalog's configs.
 */
public class HiveCatalogConfig {

	// -------------------
	// SQL Client yaml file configs
	// -------------------

	public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

	public static final String HIVE_METASTORE_USERNAME = "hive.metastore.username";

	// -------------------
	// Hive storage information configs
	// -------------------

	public static final String HIVE_TABLE_LOCATION = "hive.table.location";

	public static final String HIVE_TABLE_SERDE_LIBRARY = "hive.table.serde.library";

	public static final String HIVE_TABLE_INPUT_FORMAT = "hive.table.input.format";

	public static final String HIVE_TABLE_OUTPUT_FORMAT = "hive.table.output.format";

	public static final String HIVE_TABLE_COMPRESSED = "hive.table.compressed";

	public static final String HIVE_TABLE_NUM_BUCKETS = "hive.table.num.buckets";

	public static final String HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT = "hive.table.storage.serialization.format";
}
