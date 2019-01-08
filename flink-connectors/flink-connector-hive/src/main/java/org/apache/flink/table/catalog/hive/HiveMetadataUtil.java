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

import org.apache.flink.table.api.Column;
import org.apache.flink.table.api.RichTableSchema;
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
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.config.HiveDbConfig;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.util.PropertiesUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_DB_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_NAMES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_TYPES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TABLE_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TYPE;

/**
 * Utils for meta objects conversion between Flink and Hive.
 */
public class HiveMetadataUtil {
	/**
	 * The number of milliseconds in a day.
	 */
	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

	private HiveMetadataUtil() {
	}

	/**
	 * Create a Hive table from ExternalCatalogTable.
	 */
	public static Table createHiveTable(ObjectPath tablePath, ExternalCatalogTable table) {
		Properties prop = new Properties();
		prop.putAll(table.getProperties());

		// StorageDescriptor
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

		// Partitions
		List<FieldSchema> partitionKeys = new ArrayList<>();
		if (table.isPartitioned()) {
			LinkedHashSet<String> cols = table.getPartitionColumnNames();

			for (String col : cols) {
				FieldSchema fieldSchema = new FieldSchema(col, serdeConstants.STRING_TYPE_NAME, null);
				partitionKeys.add(fieldSchema);
			}
		}

		Table hiveTable = new Table();
		hiveTable.setSd(sd);
		hiveTable.setPartitionKeys(partitionKeys);
		hiveTable.setTableType(prop.getProperty(HIVE_TABLE_TYPE));
		hiveTable.setDbName(tablePath.getDbName());
		hiveTable.setTableName(tablePath.getObjectName());
		hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
		hiveTable.setParameters(table.getProperties());

		return hiveTable;
	}

	/**
	 * Create Hive columns from Flink TableSchema.
	 */
	private static List<FieldSchema> createHiveColumns(TableSchema schema) {
		List<FieldSchema> columns = new ArrayList<>();
		for (Column column : schema.getColumns()) {
			FieldSchema fieldSchema = new FieldSchema(column.name(), convert(column.internalType()), null);
			columns.add(fieldSchema);
		}
		return columns;
	}

	/**
	 * Create Flink's TableSchema from Hive columns.
	 */
	public static TableSchema createTableSchema(List<FieldSchema> fieldSchemas, List<FieldSchema> partitionFields) {
		int colSize = fieldSchemas.size() + partitionFields.size();

		String[] colNames = new String[colSize];
		InternalType[] colTypes = new InternalType[colSize];

		for (int i = 0; i < fieldSchemas.size(); i++) {
			FieldSchema fs = fieldSchemas.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = HiveMetadataUtil.convert(fs.getType());
		}
		for (int i = 0; i < colSize - fieldSchemas.size(); i++){
			FieldSchema fs = partitionFields.get(i);

			colNames[i + fieldSchemas.size()] = fs.getName();
			colTypes[i + fieldSchemas.size()] = HiveMetadataUtil.convert(fs.getType());
		}

		return new TableSchema(colNames, colTypes);
	}

	/**
	 * Create an ExternalCatalogTable from Hive table.
	 */
	public static ExternalCatalogTable createExternalCatalogTable(Table hiveTable, TableSchema tableSchema, TableStats tableStats) {
		return new ExternalCatalogTable(
			"hive",
			tableSchema,
			getPropertiesFromHiveTable(hiveTable),
			new RichTableSchema(tableSchema.getFieldNames(), tableSchema.getFieldTypes()),
			tableStats,
			null,
			getPartitionCols(hiveTable),
			hiveTable.getPartitionKeysSize() != 0,
			null,
			null,
			-1L,
			(long) hiveTable.getCreateTime(),
			(long) hiveTable.getLastAccessTime());
	}

	/**
	 * Create Flink TableStats from the given Hive column stats.
	 */
	public static TableStats createTableStats(Long rowCount, List<ColumnStatisticsObj> hiveColStats) {
		Map<String, ColumnStats> colStats = new HashMap<>();
		if (colStats != null && !colStats.isEmpty()) {
			for (ColumnStatisticsObj colStatsObj : hiveColStats) {
				ColumnStats columnStats = createTableColumnStats(colStatsObj.getStatsData());

				if (colStats != null) {
					colStats.put(colStatsObj.getColName(), columnStats);
				}
			}
		}

		return new TableStats(rowCount, colStats);
	}

	/**
	 * Create Flink ColumnStats from Hive ColumnStatisticsData.
	 */
	private static ColumnStats createTableColumnStats(ColumnStatisticsData statsData) {
		if (statsData.isSetBinaryStats()) {
			BinaryColumnStatsData binaryStats = statsData.getBinaryStats();
			return new ColumnStats(
				null,
				binaryStats.getNumNulls(),
				binaryStats.getAvgColLen(),
				(int) binaryStats.getMaxColLen(),
				null,
				null);
		} else if (statsData.isSetBooleanStats()) {
			BooleanColumnStatsData booleanStats = statsData.getBooleanStats();
			return new ColumnStats(
				null,
				booleanStats.getNumNulls(),
				null,
				null,
				null,
				null);
		} else if (statsData.isSetDateStats()) {
			DateColumnStatsData dateStats = statsData.getDateStats();
			return new ColumnStats(
				dateStats.getNumDVs(),
				dateStats.getNumNulls(),
				null,
				null,
				new Date(dateStats.getHighValue().getDaysSinceEpoch() * MILLIS_PER_DAY),
				new Date(dateStats.getLowValue().getDaysSinceEpoch() * MILLIS_PER_DAY));
		} else if (statsData.isSetDecimalStats()) {
			DecimalColumnStatsData decimalStats = statsData.getDecimalStats();

			Decimal highValue = decimalStats.getHighValue();
			Decimal lowValue = decimalStats.getLowValue();

			return new ColumnStats(
				decimalStats.getNumDVs(),
				decimalStats.getNumNulls(),
				null,
				null,
				new BigDecimal(new BigInteger(highValue.getUnscaled()), highValue.getScale()),
				new BigDecimal(new BigInteger(lowValue.getUnscaled()), lowValue.getScale()));
		} else if (statsData.isSetDoubleStats()) {
			DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
			return new ColumnStats(
				doubleStats.getNumDVs(),
				doubleStats.getNumNulls(),
				null,
				null,
				doubleStats.getHighValue(),
				doubleStats.getLowValue());
		} else if (statsData.isSetLongStats()) {
			LongColumnStatsData longStats = statsData.getLongStats();
			return new ColumnStats(
				longStats.getNumDVs(),
				longStats.getNumNulls(),
				null,
				null,
				longStats.getHighValue(),
				longStats.getLowValue());
		} else if (statsData.isSetStringStats()) {
			StringColumnStatsData stringStats = statsData.getStringStats();
			return new ColumnStats(
				stringStats.getNumDVs(),
				stringStats.getNumNulls(),
				stringStats.getAvgColLen(),
				(int) stringStats.getMaxColLen(),
				null,
				null);
		}
		return null;
	}

	private static LinkedHashSet<String> getPartitionCols(Table hiveTable) {
		return hiveTable.getPartitionKeys().stream()
			.map(fs -> fs.getName())
			.collect(Collectors.toCollection(LinkedHashSet::new));
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
		prop.put(HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT,
				String.valueOf(sd.getSerdeInfo().getParameters().get(serdeConstants.SERIALIZATION_FORMAT)));

		prop.putAll(table.getParameters());

		List<FieldSchema> fieldSchemas = new ArrayList<>(sd.getCols());
		fieldSchemas.addAll(table.getPartitionKeys());
		String[] colNames = new String[fieldSchemas.size()];
		String[] hiveTypes = new String[fieldSchemas.size()];
		for (int i = 0; i < fieldSchemas.size(); i++) {
			colNames[i] = fieldSchemas.get(i).getName();
			hiveTypes[i] = fieldSchemas.get(i).getType();
		}
		prop.put(HIVE_TABLE_FIELD_NAMES, StringUtils.join(colNames, ","));
		prop.put(HIVE_TABLE_FIELD_TYPES, StringUtils.join(hiveTypes, ","));
		prop.put(HIVE_TABLE_DB_NAME, table.getDbName());
		prop.put(HIVE_TABLE_TABLE_NAME, table.getTableName());
		prop.put(HIVE_TABLE_PARTITION_FIELDS, String.valueOf(
				StringUtils.join(
						table.getPartitionKeys()
							.stream()
							.map(key -> key.getName().toLowerCase())
							.collect(Collectors.toList()), ",")));

		return prop;
	}

	/**
	 * Create a Hive database from CatalogDatabase.
	 */
	public static Database createHiveDatabase(String dbName, CatalogDatabase catalogDb) {
		Map<String, String> prop = catalogDb.getProperties();

		return new Database(
			dbName,
			prop.get(HiveDbConfig.HIVE_DB_DESCRIPTION),
			prop.get(HiveDbConfig.HIVE_DB_LOCATION_URI),
			catalogDb.getProperties());
	}

	/**
	 * Create a Hive database from CatalogDatabase.
	 */
	public static CatalogDatabase createCatalogDatabase(Database hiveDb) {
		Map<String, String> prop = new HashMap<>(hiveDb.getParameters());

		prop.put(HiveDbConfig.HIVE_DB_LOCATION_URI, hiveDb.getLocationUri());
		prop.put(HiveDbConfig.HIVE_DB_DESCRIPTION, hiveDb.getDescription());
		prop.put(HiveDbConfig.HIVE_DB_OWNER_NAME, hiveDb.getOwnerName());

		return new CatalogDatabase(prop);
	}

	/**
	 * Create a CatalogPartition from the given PartitionSpec and Hive partition.
	 */
	public static CatalogPartition createCatalogPartition(CatalogPartition.PartitionSpec spec, Partition partition) {
		Map<String, String> prop = new HashMap<>(partition.getParameters());

		StorageDescriptor sd = partition.getSd();
		prop.put(HIVE_TABLE_LOCATION, sd.getLocation());
		prop.put(HIVE_TABLE_SERDE_LIBRARY, sd.getSerdeInfo().getSerializationLib());
		prop.put(HIVE_TABLE_INPUT_FORMAT, sd.getInputFormat());
		prop.put(HIVE_TABLE_OUTPUT_FORMAT, sd.getOutputFormat());
		prop.put(HIVE_TABLE_COMPRESSED, String.valueOf(sd.isCompressed()));
		prop.put(HIVE_TABLE_NUM_BUCKETS, String.valueOf(sd.getNumBuckets()));

		prop.putAll(partition.getParameters());

		return new CatalogPartition(spec, prop);
	}

	/**
	 * Create Flink PartitionSpec from Hive partition name string.
	 * Example of Hive partition name string - "name=bob/year=2019"
	 */
	public static CatalogPartition.PartitionSpec createPartitionSpec(String hivePartitionName) {
		return CatalogPartition.fromStrings(Arrays.asList(hivePartitionName.split("/")));
	}

	/**
	 * Create a Hive partition from the given Hive table and CatalogPartition.
	 */
	public static Partition createHivePartition(Table hiveTable, CatalogPartition cp) {
		Partition partition = new Partition();

		partition.setValues(cp.getPartitionSpec().getOrderedValues(getPartitionKeys(hiveTable.getPartitionKeys())));
		partition.setDbName(hiveTable.getDbName());
		partition.setTableName(hiveTable.getTableName());
		partition.setCreateTime((int) (System.currentTimeMillis() / 1000));
		partition.setParameters(cp.getProperties());
		partition.setSd(hiveTable.getSd().deepCopy());

		String location = cp.getProperties().get(HIVE_TABLE_LOCATION);
		partition.getSd().setLocation(location != null ? location : null);

		return partition;
	}

	/**
	 * Get Hive table partition keys.
	 */
	public static List<String> getPartitionKeys(List<FieldSchema> fieldSchemas) {
		List<String> partitionKeys = new ArrayList<>(fieldSchemas.size());

		for (FieldSchema fs : fieldSchemas) {
			partitionKeys.add(fs.getName());
		}

		return partitionKeys;
	}

	/**
	 * Convert a hive type to Flink internal type.
	 */

	public static InternalType convert(String hiveType) {
		// First, handle types that have parameters such as CHAR(5), DECIMAL(6, 2), etc
		if (hiveType.toLowerCase().startsWith(serdeConstants.CHAR_TYPE_NAME) ||
			hiveType.toLowerCase().startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
			// For CHAR(p) and VARCHAR(p) types, map them to String for now because Flink doesn't yet support them.
			return StringType.INSTANCE;
		} else if (hiveType.toLowerCase().startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
			return DecimalType.of(hiveType);
		}

		switch (hiveType) {
			case serdeConstants.STRING_TYPE_NAME:
				return StringType.INSTANCE;
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
			return serdeConstants.CHAR_TYPE_NAME + "(1)";
		} else if (internalType.equals(DateType.DATE)) {
			return serdeConstants.DATE_TYPE_NAME;
		} else if (internalType.equals(TimeType.INSTANCE)) {
			return serdeConstants.DATE_TYPE_NAME;
		} else if (internalType instanceof TimestampType) {
			return serdeConstants.TIMESTAMP_TYPE_NAME;
		} else if (internalType instanceof DecimalType) {
			return internalType.toString();
		} else if (internalType.equals(ByteArrayType.INSTANCE)) {
			return serdeConstants.BINARY_TYPE_NAME;
		} else {
			throw new UnsupportedOperationException(
				String.format("Flink's hive metadata integration doesn't support Flink type %s yet.",
					internalType.toString()));
		}
	}
}
