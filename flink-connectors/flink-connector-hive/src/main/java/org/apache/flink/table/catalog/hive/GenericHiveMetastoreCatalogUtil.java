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

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.config.GenericHmsTableConfig.PARTITION_KEYS;
import static org.apache.flink.table.catalog.hive.config.GenericHmsTableConfig.PARTITION_KEYS_DELIMITER;
import static org.apache.flink.table.catalog.hive.config.GenericHmsTableConfig.TABLE_TYPE;

/**
 * Util for GenericHiveMetastoreCatalog.
 */
public class GenericHiveMetastoreCatalogUtil {

	/**
	 * Create a Hive table from CatalogTable.
	 * Note that create Hive table doesn't include TableStats
	 */
	protected static Table createHiveTable(ObjectPath tablePath, CatalogTable table) {
		Map<String, String> properties = new HashMap<>();
		Schema schema = new Schema().schema(table.getTableSchema());
		properties.putAll(schema.toProperties());

		// Table type
		properties.put(TABLE_TYPE, table.getTableType());

		// Partitioned keys
		if (table.isPartitioned()) {
			properties.put(PARTITION_KEYS, String.join(PARTITION_KEYS_DELIMITER, table.getPartitionColumnNames()));
		}

		// StorageDescriptor
		StorageDescriptor sd = new StorageDescriptor();
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));
		sd.setCols(new ArrayList<>());

		Table hiveTable = new Table();
		hiveTable.setSd(sd);
		hiveTable.setDbName(tablePath.getDbName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));

		hiveTable.setParameters(properties);

		return hiveTable;
	}

	protected static CatalogTable createCatalogTable(Table table) {
		DescriptorProperties descProp = new DescriptorProperties();
		descProp.putProperties(getPropertiesWithStartingKey(table.getParameters(), SchemaValidator.SCHEMA()));

		// TableSchema
		TableSchema tableSchema = descProp.getTableSchema(SchemaValidator.SCHEMA());

		// Properties
		Map<String, String> properties = getPropertiesWithoutStartingKeys(table.getParameters(), new HashSet<String>() {{
			add(SchemaValidator.SCHEMA());
		}});

		// Partition keys
		LinkedHashSet<String> partitionKeys = new LinkedHashSet<>();

		if (table.getParameters().containsKey(PARTITION_KEYS)) {
			partitionKeys = Arrays.stream(table.getParameters().get(PARTITION_KEYS).split(PARTITION_KEYS_DELIMITER))
				.collect(Collectors.toCollection(LinkedHashSet::new));
		}

		return new CatalogTable(
			table.getParameters().get(TABLE_TYPE),
			tableSchema,
			properties,
			new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes()),
			new TableStats(),
			null,
			partitionKeys,
			!partitionKeys.isEmpty(),
			null,
			null,
			-1L,
			(long) table.getCreateTime(),
			(long) table.getLastAccessTime(),
			false
		);
	}

	private static Map<String, String> getPropertiesWithStartingKey(Map<String, String> prop, String prefix) {
		String prefixWithDot = prefix + ".";

		return prop.entrySet().stream()
			.filter(e -> e.getKey().startsWith(prefixWithDot))
			.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}

	private static Map<String, String> getPropertiesWithoutStartingKeys(Map<String, String> prop, Set<String> prefixes) {
		return prop.entrySet().stream()
			.filter(e -> !prefixes.contains(e.getKey().split("\\.")[0]))
			.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}
}
