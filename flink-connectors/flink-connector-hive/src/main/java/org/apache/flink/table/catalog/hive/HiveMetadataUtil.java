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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Column;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteArrayType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.CharType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimeType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.config.HiveDbConfig;
import org.apache.flink.util.PropertiesUtil;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TYPE;

/**
 * Convert Hive data type to Blink data type.
 */
public class HiveMetadataUtil {
	/**
	 * Create a hive table from ExternalCatalogTable.
	 */
	public static Table createHiveTable(ObjectPath tablePath, ExternalCatalogTable table) {
		Properties prop = new Properties();
		prop.putAll(table.getProperties());

		StorageDescriptor sd = new StorageDescriptor();
		sd.setInputFormat(prop.getProperty(HIVE_TABLE_INPUT_FORMAT));
		sd.setOutputFormat(prop.getProperty(HIVE_TABLE_OUTPUT_FORMAT));
		sd.setLocation(prop.getProperty(HIVE_TABLE_LOCATION));
		sd.setSerdeInfo(
			new SerDeInfo(
				null,
				prop.getProperty(HIVE_TABLE_SERDE_LIBRARY, LazySimpleSerDe.class.getName()),
				new HashMap<>())
		);
		sd.setCompressed(Boolean.valueOf(prop.getProperty(HIVE_TABLE_COMPRESSED)));
		sd.setParameters(new HashMap<>());
		sd.setNumBuckets(PropertiesUtil.getInt(prop, HIVE_TABLE_NUM_BUCKETS, -1));
		sd.setBucketCols(new ArrayList<>());
		sd.setCols(createHiveColumns(table.getTableSchema()));
		sd.setSortCols(new ArrayList<>());

		Table t = new Table();
		t.setSd(sd);
		t.setPartitionKeys(new ArrayList<>());
		t.setParameters(new HashMap<>());
		t.setTableType(prop.getProperty(HIVE_TABLE_TYPE));
		t.setDbName(tablePath.getDbName());
		t.setTableName(tablePath.getObjectName());
		t.setCreateTime((int) (System.currentTimeMillis() / 1000));
		t.setParameters(table.getProperties());

		return t;
	}

	private static List<FieldSchema> createHiveColumns(TableSchema schema) {
		List<FieldSchema> columns = new ArrayList<>();
		for (Column column : schema.getColumns()) {
			FieldSchema fieldSchema = new FieldSchema(column.name(), convert(column.internalType()), null);
			columns.add(fieldSchema);
		}
		return columns;
	}

	/**
	 * Create an ExternalCatalogTable from hive table.
	 */
	public static ExternalCatalogTable createExternalCatalogTable(Table hiveTable) {
		return new ExternalCatalogTable(
			"hive",
			createTableSchema(hiveTable.getSd().getCols()),
			getPropertiesFromHiveTable(hiveTable),
			null,
			null,
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			(long) hiveTable.getCreateTime(),
			(long) hiveTable.getLastAccessTime());
	}

	private static Map<String, String> getPropertiesFromHiveTable(Table table) {
		Map<String, String> prop = new HashMap<>(table.getParameters());

		prop.put(HIVE_TABLE_TYPE, table.getTableType());

		StorageDescriptor sd = table.getSd();
		prop.put(HIVE_TABLE_LOCATION, sd.getLocation());
		prop.put(HIVE_TABLE_SERDE_LIBRARY, sd.getSerdeInfo().getSerializationLib());
		prop.put(HIVE_TABLE_INPUT_FORMAT, sd.getInputFormat());
		prop.put(HIVE_TABLE_OUTPUT_FORMAT, sd.getOutputFormat());
		prop.put(HIVE_TABLE_COMPRESSED, String.valueOf(sd.isCompressed()));
		prop.put(HIVE_TABLE_NUM_BUCKETS, String.valueOf(sd.getNumBuckets()));

		prop.putAll(table.getParameters());

		return prop;
	}

	@VisibleForTesting
	protected static TableSchema createTableSchema(List<FieldSchema> fieldSchemas) {
		int colSize = fieldSchemas.size();

		String[] colNames = new String[colSize];
		InternalType[] colTypes = new InternalType[colSize];

		for (int i = 0; i < colSize; i++) {
			FieldSchema fs = fieldSchemas.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = convert(fs.getType());
		}

		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Create a hive database from CatalogDatabase.
	 */
	public static Database createHiveDatabase(String dbName, CatalogDatabase catalogDb) {
		Map<String, String> prop = catalogDb.getProperties();

		Database db = new Database(
			dbName,
			prop.get(HiveDbConfig.HIVE_DB_DESCRIPTION),
			prop.get(HiveDbConfig.HIVE_DB_LOCATION),
			catalogDb.getProperties());
		return db;
	}

	/**
	 * Create a hive database from CatalogDatabase.
	 */
	public static CatalogDatabase createCatalogDatabase(Database hiveDb) {
		Map<String, String> prop = new HashMap<>(hiveDb.getParameters());

		// TODO: enable comparing CatalogDatabase
//		prop.put(HiveDbConfig.HIVE_DB_LOCATION, hiveDb.getLocationUri());
//		prop.put(HiveDbConfig.HIVE_DB_DESCRIPTION, hiveDb.getDescription());
//		prop.put(HiveDbConfig.HIVE_DB_OWNER, hiveDb.getOwnerName());

		return new CatalogDatabase(prop);
	}

	/**
	 * Convert a hive type to Flink internal type.
	 */

	public static InternalType convert(String hiveType) {
		switch (hiveType) {
			case serdeConstants.STRING_TYPE_NAME:
				return StringType.INSTANCE;
			case serdeConstants.CHAR_TYPE_NAME:
				return CharType.INSTANCE;
			case serdeConstants.BOOLEAN_TYPE_NAME:
				return BooleanType.INSTANCE;
			case serdeConstants.TINYINT_TYPE_NAME:
				return ByteType.INSTANCE;
			case serdeConstants.SMALLINT_TYPE_NAME:
				return ShortType.INSTANCE;
			case serdeConstants.INT_TYPE_NAME:
				return IntType.INSTANCE;
			case serdeConstants.BIGINT_TYPE_NAME:
				return LongType.INSTANCE;
			case serdeConstants.FLOAT_TYPE_NAME:
				return FloatType.INSTANCE;
			case serdeConstants.DOUBLE_TYPE_NAME:
				return DoubleType.INSTANCE;
			case serdeConstants.DATE_TYPE_NAME:
				return DateType.DATE;
			case serdeConstants.DATETIME_TYPE_NAME:
				return TimeType.INSTANCE;
			case serdeConstants.TIMESTAMP_TYPE_NAME:
				return TimestampType.TIMESTAMP;
			case serdeConstants.DECIMAL_TYPE_NAME:
				return DecimalType.SYSTEM_DEFAULT;
			case serdeConstants.BINARY_TYPE_NAME:
				return ByteArrayType.INSTANCE;
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive's type %s yet.", hiveType));
		}
	}

	/**
	 * Convert Flink's internal type to String for hive.
	 */
	private static String convert(InternalType internalType) {
		if (internalType.equals(BooleanType.INSTANCE)) {
			return serdeConstants.BOOLEAN_TYPE_NAME;
		} else if (internalType.equals(ByteType.INSTANCE)) {
			return serdeConstants.TINYINT_TYPE_NAME;
		} else if (internalType.equals(ShortType.INSTANCE)) {
			return serdeConstants.SMALLINT_TYPE_NAME;
		} else if (internalType.equals(IntType.INSTANCE)) {
			return serdeConstants.INT_TYPE_NAME;
		} else if (internalType.equals(LongType.INSTANCE)) {
			return serdeConstants.BIGINT_TYPE_NAME;
		} else if (internalType.equals(FloatType.INSTANCE)) {
			return serdeConstants.FLOAT_TYPE_NAME;
		} else if (internalType.equals(DoubleType.INSTANCE)) {
			return serdeConstants.DOUBLE_TYPE_NAME;
		} else if (internalType.equals(StringType.INSTANCE)) {
			return serdeConstants.STRING_TYPE_NAME;
		} else if (internalType.equals(CharType.INSTANCE)) {
			return serdeConstants.CHAR_TYPE_NAME;
		} else if (internalType.equals(DateType.DATE)) {
			return serdeConstants.DATE_TYPE_NAME;
		} else if (internalType.equals(TimeType.INSTANCE)) {
			return serdeConstants.DATETIME_TYPE_NAME;
		} else if (internalType instanceof TimestampType) {
			return serdeConstants.TIMESTAMP_TYPE_NAME;
		} else if (internalType instanceof DecimalType) {
			return serdeConstants.DECIMAL_TYPE_NAME;
		} else if (internalType.equals(ByteArrayType.INSTANCE)) {
			return serdeConstants.BINARY_TYPE_NAME;
		} else {
			throw new UnsupportedOperationException(
				String.format("Flink's hive metadata integration doesn't support Flink type %s yet.",
					internalType.toString()));
		}
	}
}
