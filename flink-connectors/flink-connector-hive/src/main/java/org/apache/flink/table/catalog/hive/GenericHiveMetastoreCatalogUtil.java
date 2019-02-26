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
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogTableConfig;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.metastore.TableType;
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
	private static final Map<String, String> EXTERNAL_TABLE_PROPERTY = new HashMap<String, String>() {{
		put("EXTERNAL", "TRUE");
	}};

	/**
	 * Create a Hive external table from CatalogTable or CatalogView.
	 * Note that create Hive table doesn't include TableStats
	 */
	static Table createHiveTable(ObjectPath path, CatalogTable table) {
		Map<String, String> properties = table.getProperties();

		// Table Schema
		Schema schema = new Schema().schema(table.getTableSchema());
		properties.putAll(schema.toProperties());

		// Table type
		properties.put(TABLE_TYPE, table.getTableType());

		// Partitioned keys
		if (table.isPartitioned()) {
			properties.put(PARTITION_KEYS, String.join(PARTITION_KEYS_DELIMITER, table.getPartitionColumnNames()));
		}

		// Table comment
		if (table.getComment() != null) {
			properties.put(CatalogTableConfig.TABLE_COMMENT, table.getComment());
		}

		// StorageDescriptor
		StorageDescriptor sd = new StorageDescriptor();
		sd.setSerdeInfo(new SerDeInfo(null, null, new HashMap<>()));
		sd.setCols(new ArrayList<>());

		Table hiveTable = new Table();
		hiveTable.setSd(sd);
		hiveTable.setDbName(path.getDbName());
		hiveTable.setTableName(path.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
		hiveTable.setPartitionKeys(new ArrayList<>());
		hiveTable.setParameters(properties);
		hiveTable.getParameters().putAll(EXTERNAL_TABLE_PROPERTY);

		// Distinguish Table v.s. Virtual View
		if (table instanceof CatalogView) {
			CatalogView view = (CatalogView) table;
			hiveTable.setViewOriginalText(view.getOriginalQuery());
			hiveTable.setViewExpandedText(view.getExpandedQuery());
			hiveTable.setTableType(TableType.VIRTUAL_VIEW.name());
		} else {
			hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
		}

		return hiveTable;
	}

	/**
	 * Create a CatalogTable from Hive table.
	 * Note that create Hive table doesn't include TableStats
	 */
	static CatalogTable createCatalogTable(Table hiveTable) {
		DescriptorProperties descProp = new DescriptorProperties();
		descProp.putProperties(getPropertiesWithStartingKey(hiveTable.getParameters(), SchemaValidator.SCHEMA()));

		// TableSchema
		TableSchema tableSchema = descProp.getTableSchema(SchemaValidator.SCHEMA());

		// Properties
		Map<String, String> properties = getPropertiesWithoutStartingKeys(hiveTable.getParameters(), new HashSet<String>() {{
			add(SchemaValidator.SCHEMA());
		}});

		// Table comment
		String tableComment = properties.remove(CatalogTableConfig.TABLE_COMMENT);

		// Partition keys
		LinkedHashSet<String> partitionKeys = new LinkedHashSet<>();

		if (hiveTable.getParameters().containsKey(PARTITION_KEYS)) {
			partitionKeys = Arrays.stream(hiveTable.getParameters().get(PARTITION_KEYS).split(PARTITION_KEYS_DELIMITER))
				.collect(Collectors.toCollection(LinkedHashSet::new));
		}
		TableStats tableStats;
		if (partitionKeys.isEmpty()) {
			// rowCnt is 0 for new created hive table
			tableStats = TableStats.builder().rowCount(0L).build();
		} else {
			// TableStats of partitioned table is unknown, the behavior is same as HIVE
			tableStats = TableStats.UNKNOWN();
		}

		CatalogTable table = new CatalogTable(
			hiveTable.getParameters().get(TABLE_TYPE),
			tableSchema,
			properties,
			new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes()),
			tableStats,
			tableComment,
			partitionKeys,
			!partitionKeys.isEmpty(),
			null,
			null,
			-1L,
			(long) hiveTable.getCreateTime(),
			(long) hiveTable.getLastAccessTime()
		);

		if (TableType.valueOf(hiveTable.getTableType()) == TableType.VIRTUAL_VIEW) {
			return CatalogView.createCatalogView(table, hiveTable.getViewOriginalText(), hiveTable.getViewExpandedText());
		} else {
			return table;
		}
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
