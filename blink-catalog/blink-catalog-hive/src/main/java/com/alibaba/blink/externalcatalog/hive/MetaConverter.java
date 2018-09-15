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

package com.alibaba.blink.externalcatalog.hive;

import org.apache.flink.table.api.Column;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTablePartition;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TablePartitionStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.BooleanType;
import org.apache.flink.table.types.ByteType;
import org.apache.flink.table.types.CharType;
import org.apache.flink.table.types.DateType;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.DoubleType;
import org.apache.flink.table.types.FloatType;
import org.apache.flink.table.types.IntType;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.LongType;
import org.apache.flink.table.types.ShortType;
import org.apache.flink.table.types.StringType;
import org.apache.flink.table.types.TimeType;
import org.apache.flink.table.types.TimestampType;
import org.apache.flink.util.StringUtils;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * A Meta Converter which converts hive table and flink external table.
 */
public class MetaConverter {

	private static final Logger LOGGER = LoggerFactory.getLogger(MetaConverter.class);

	private static final String FLINK_TABLE_TYPE = "flink.table.type";
	private static final String SEQUENCE_FILE_CLASS = "org.apache.hadoop.mapred.SequenceFileInputFormat";

	private MetaConverter() { }

	static Table convertToHiveTable(
			ExternalCatalogTable table,
			String databaseName,
			String tableName) {

		// See CreateTableDesc.java in hive
		Table hTable = getEmptyTable(databaseName, tableName);

		hTable.setParameters(table.properties());
		hTable.putToParameters(FLINK_TABLE_TYPE, table.tableType());

		// Default PartCols = null, NumBuckets = -1
		// Default Storage Handler = null
		// Default Serde is LazySimpleSerDe
		String serDeClassName = LazySimpleSerDe.class.getName();
		hTable.getSd().getSerdeInfo().setSerializationLib(serDeClassName);

		List<FieldSchema> cols = getHiveCols(table.schema());
		hTable.getSd().setCols(cols);

		if (table.isPartitioned()) {
			LinkedHashSet<String> colNames = table.partitionColumnNames();
			List<FieldSchema> parts = new ArrayList<>();
			for (String colName : colNames) {
				FieldSchema part = new FieldSchema(colName, serdeConstants.STRING_TYPE_NAME, "");
				parts.add(part);
			}
			hTable.setPartitionKeys(parts);
		}

		return hTable;
	}

	static ExternalCatalogTable convertToExternalCatalogTable(
			Table table,
			IMetaStoreClient msc) throws TException {

		String tableType = table.getParameters().get(FLINK_TABLE_TYPE);
		if (StringUtils.isNullOrWhitespaceOnly(tableType)) {
			tableType = "hive";
		}
		List<FieldSchema> fields = table.getSd().getCols();
		TableSchema tableSchema = getTableSchema(fields);

		// Table Level Statistics
		long rowCount = 0;
		String numRows = table.getParameters().get(StatsSetupConst.ROW_COUNT);
		if (!StringUtils.isNullOrWhitespaceOnly(numRows)) {
			rowCount = Long.valueOf(numRows);
		}

		TableStats tableStats;
		if (table.getPartitionKeys() != null &&
				table.getPartitionKeys().size() > 0) {
			// Partitioned Table
			tableStats = null;
		} else {
			// Managed table
			tableStats = new TableStats(rowCount, new HashMap<>());

			// Column Level Statistics
			if (msc != null) {
				// Managed Table, Non Partition Table
				String[] cols = tableSchema.getColumnNames();
				List<String> colNames = Arrays.asList(cols);
				List<ColumnStatisticsObj> stats = msc.getTableColumnStatistics(
						table.getDbName(), table.getTableName(), colNames);
				if (null != stats && stats.size() > 0) {
					Map<String, ColumnStats> colStats = tableStats.colStats();
					fillTheColStats(colStats, stats, tableSchema);
				}
			}
		}

		return new ExternalCatalogTable(
				tableType,
				tableSchema,
				table.getParameters(),
				tableStats,
				table.getDbName() + "." + table.getTableName(),
				getPartitionColumnNames(table.getPartitionKeys()),
				table.getPartitionKeys() != null
						&& table.getPartitionKeys().size() > 0,
				table.getCreateTime() * 1000L,
				table.getLastAccessTime() * 1000L);
	}

	static ExternalCatalogTablePartition convertToExternalCatalogPartition(
			Partition part,
			LinkedHashMap<String, String> partSpec,
			IMetaStoreClient msc) throws TException {

		TablePartitionStats stats = null;
		if (msc != null) {
			// Table Level Stats
			long rowCount = 0;
			String numRows = part.getParameters().get(StatsSetupConst.ROW_COUNT);
			if (!StringUtils.isNullOrWhitespaceOnly(numRows)) {
				rowCount = Long.valueOf(numRows);
			}
			stats = new TablePartitionStats(rowCount, new HashMap<>());

			// Column Level Stats
			Table table = msc.getTable(part.getDbName(), part.getTableName());
			List<FieldSchema> fields = table.getSd().getCols();
			TableSchema tableSchema = getTableSchema(fields);
			List<String> colNames = new ArrayList<>();
			for (FieldSchema field : fields) {
				colNames.add(field.getName());
			}
			String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
			List<String> partNames = new ArrayList<>();
			partNames.add(partName);
			List<ColumnStatisticsObj> partStats = msc.getPartitionColumnStatistics(
					part.getDbName(), part.getTableName(), partNames, colNames).get(partName);

			if (partStats != null && partStats.size() > 0) {
				Map<String, ColumnStats> colStats = stats.colStats();
				fillTheColStats(colStats, partStats, tableSchema);
			}
		}

		return new ExternalCatalogTablePartition(partSpec, part.getParameters(), stats);
	}

	/**
	 * Fill the column-level statistics hash map.
	 *
	 * @param colStats The Map to be filled.
	 * @param stats The Statistics from Hive Metastore API.
	 * @param tableSchema The table schema.
	 */
	public static void fillTheColStats(
			Map<String, ColumnStats> colStats,
			List<ColumnStatisticsObj> stats,
			TableSchema tableSchema) {

		for (ColumnStatisticsObj statisticsObj : stats) {
			int colIndex = (int) tableSchema.columnNameToIndex()
					.get(statisticsObj.getColName()).get();
			if (colIndex >= 0 && colIndex < tableSchema.getColumns().length) {
				Column column = tableSchema.getColumn(colIndex);
				InternalType flinkType = column.internalType();
				ColumnStats columnStats = convertHiveStatsToFlinkStats(
						statisticsObj, flinkType);
				colStats.put(statisticsObj.getColName(), columnStats);
			}
		}
	}

	/**
	 * Convert the Hive Statistics to the Flink Statistics according to the Flink Type.
	 *
	 * @param statsObj Hive Statistics Object
	 * @param flinkType The flink type
	 * @return The Flink Statistics Object
	 */
	public static ColumnStats convertHiveStatsToFlinkStats(
			ColumnStatisticsObj statsObj,
			InternalType flinkType) {

		ColumnStatisticsData statsData = statsObj.getStatsData();
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
					convertToJava(
							HiveStatsValueType.DATE, flinkType, dateStats.getHighValue()),
					convertToJava(
							HiveStatsValueType.DATE, flinkType, dateStats.getLowValue()));
		} else if (statsData.isSetDecimalStats()) {
			DecimalColumnStatsData decimalStats = statsData.getDecimalStats();
			return new ColumnStats(
					decimalStats.getNumDVs(),
					decimalStats.getNumNulls(),
					null,
					null,
					convertToJava(
							HiveStatsValueType.DECIMAL, flinkType, decimalStats.getHighValue()),
					convertToJava(
							HiveStatsValueType.DECIMAL, flinkType, decimalStats.getLowValue()));
		} else if (statsData.isSetDoubleStats()) {
			DoubleColumnStatsData doubleStats = statsData.getDoubleStats();
			return new ColumnStats(
					doubleStats.getNumDVs(),
					doubleStats.getNumNulls(),
					null,
					null,
					convertToJava(
							HiveStatsValueType.DOUBLE, flinkType, doubleStats.getHighValue()),
					convertToJava(
							HiveStatsValueType.DOUBLE, flinkType, doubleStats.getLowValue()));
		} else if (statsData.isSetLongStats()) {
			LongColumnStatsData longStats = statsData.getLongStats();
			return new ColumnStats(
					longStats.getNumDVs(),
					longStats.getNumNulls(),
					null,
					null,
					convertToJava(
							HiveStatsValueType.LONG, flinkType, longStats.getHighValue()),
					convertToJava(
							HiveStatsValueType.LONG, flinkType, longStats.getLowValue()));
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

	/**
	 * Convert a Hive Thrift Date to java.sql.Date.
	 * See SelectivityEstimator.comparableToDouble()
	 *
	 * @param hiveDate a Hive Thrift Date
	 * @return a Java SQL Date
	 */
	private static java.sql.Date convertToJavaDate(Date hiveDate)  {
		return new java.sql.Date(
				hiveDate.getDaysSinceEpoch() * DateTimeUtils.MILLIS_PER_DAY);
	}

	/**
	 * Convert a Hive Thrift Decimal to java BigDecimal.
	 * See SelectivityEstimator.comparableToDouble()
	 *
	 * @param hiveDecimal a Hive Thrift Date
	 * @return a Java BigDecimal
	 */
	private static BigDecimal convertToBigDecimal(Decimal hiveDecimal) {
		return new BigDecimal(
				new BigInteger(hiveDecimal.getUnscaled()),
				hiveDecimal.getScale());
	}

	public static TableSchema getTableSchema(List<FieldSchema> hColumns) {
		String[] names = new String[hColumns.size()];
		InternalType[] internalTypes = new InternalType[hColumns.size()];
		int i = 0;
		for (FieldSchema fieldSchema : hColumns) {
			names[i] = fieldSchema.getName();
			internalTypes[i] = getInternalType(fieldSchema);
			i++;
		}
		return new TableSchema(names, internalTypes);
	}

	private static LinkedHashSet<String> getPartitionColumnNames(
			List<FieldSchema> partitionKeys) {
		LinkedHashSet<String> partitionColumnNames = new LinkedHashSet<>();
		if (null != partitionKeys && !partitionKeys.isEmpty()) {
			for (FieldSchema fieldSchema : partitionKeys) {
				partitionColumnNames.add(fieldSchema.getName());
			}
		}
		return partitionColumnNames;
	}

	private static List<FieldSchema> getHiveCols(TableSchema schema) {
		List<FieldSchema> hColumns = new ArrayList<>();
		for (Column column : schema.getColumns()) {
			FieldSchema fieldSchema = new FieldSchema();
			fieldSchema.setName(column.name());
			fieldSchema.setType(getHiveType(column.internalType()));
			hColumns.add(fieldSchema);
		}
		return hColumns;
	}

	static Table getEmptyTable(String databaseName, String tableName) {
		StorageDescriptor sd = new StorageDescriptor();
		{
			sd.setSerdeInfo(new SerDeInfo());
			sd.setNumBuckets(-1);
			sd.setBucketCols(new ArrayList<>());
			sd.setCols(new ArrayList<>());
			sd.setParameters(new HashMap<>());
			sd.setSortCols(new ArrayList<>());
			sd.getSerdeInfo().setParameters(new HashMap<>());
			// We have to use MetadataTypedColumnsetSerDe because LazySimpleSerDe does
			// not support a table with no columns.
			sd.getSerdeInfo().setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());
			sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
			sd.setInputFormat(SEQUENCE_FILE_CLASS);
			sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat");
			SkewedInfo skewInfo = new SkewedInfo();
			skewInfo.setSkewedColNames(new ArrayList<>());
			skewInfo.setSkewedColValues(new ArrayList<>());
			skewInfo.setSkewedColValueLocationMaps(new HashMap<>());
			sd.setSkewedInfo(skewInfo);
		}

		org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
		{
			t.setSd(sd);
			t.setPartitionKeys(new ArrayList<>());
			t.setParameters(new HashMap<>());
			t.setTableType(TableType.MANAGED_TABLE.toString());
			t.setDbName(databaseName);
			t.setTableName(tableName);
			t.setOwner(null);
			// set create time
			t.setCreateTime((int) (System.currentTimeMillis() / 1000));
		}
		return t;
	}

	/**
	 * Convert the Hive Stats Value to Java Value which flink type needs.
	 * @param hiveStatsValueType LONG, DECIMAL, DOUBLE or DATE
	 * @param internalType The flink internal type.
	 * @param hiveStatsValue The value
	 * @return Java Value
	 */
	private static Object convertToJava(
			HiveStatsValueType hiveStatsValueType,
			InternalType internalType,
			Object hiveStatsValue) {

		if (internalType.equals(BooleanType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(BooleanType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(ByteType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
					return ((Long) hiveStatsValue).byteValue();
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(ByteType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(ShortType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
					return ((Long) hiveStatsValue).shortValue();
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(ShortType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(IntType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
					return ((Long) hiveStatsValue).intValue();
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(IntType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(LongType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
					return hiveStatsValue;
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(LongType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(FloatType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case DOUBLE:
					return ((Double) hiveStatsValue).floatValue();
				case LONG:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(FloatType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(DoubleType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case DOUBLE:
					return hiveStatsValue;
				case LONG:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(DoubleType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(StringType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(StringType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(CharType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case LONG:
					return (char) ((Long) hiveStatsValue).longValue();
				case DOUBLE:
				case DATE:
				case DECIMAL:
				default:
					logConvertError(CharType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(DateType.DATE)) {
			switch (hiveStatsValueType) {
				case DATE:
					return convertToJavaDate((Date) hiveStatsValue);
				case LONG:
					return new java.sql.Date((Long) hiveStatsValue);
				case DECIMAL:
				case DOUBLE:
				default:
					logConvertError(DateType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType.equals(TimeType.INSTANCE)) {
			switch (hiveStatsValueType) {
				case DATE:
					return new Time(
							((Date) hiveStatsValue).getDaysSinceEpoch() * DateTimeUtils.MILLIS_PER_DAY);
				case LONG:
					return new Time((Long) hiveStatsValue);
				case DOUBLE:
				case DECIMAL:
				default:
					logConvertError(TimeType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType instanceof TimestampType) {
			switch (hiveStatsValueType) {
				case LONG:
					return new Timestamp((Long) hiveStatsValue);
				case DATE:
					return new Timestamp((
							(Date) hiveStatsValue).getDaysSinceEpoch() * DateTimeUtils.MILLIS_PER_DAY);
				case DOUBLE:
				case DECIMAL:
				default:
					logConvertError(TimestampType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else if (internalType instanceof DecimalType) {
			switch (hiveStatsValueType) {
				case DECIMAL:
					return convertToBigDecimal((Decimal) hiveStatsValue);
				case LONG:
				case DOUBLE:
				case DATE:
				default:
					logConvertError(DecimalType.class, hiveStatsValueType, hiveStatsValue);
					return null;
			}
		} else {
			throw new UnsupportedOperationException("Unsupported type information: "
					+ internalType.toString());
		}
	}

	private static void logConvertError(
			Class flinkType,
			HiveStatsValueType hiveStatsValueType,
			Object hiveStatsValue) {
		LOGGER.error("Required Flink type {}, find Hive type {} in statistics, value={}",
				flinkType.getSimpleName(), hiveStatsValueType, hiveStatsValue);
	}

	/**
	 * Get the hive type.
	 *
	 * @param internalType flink internal type
	 * @return hive type string
	 */
	private static String getHiveType(InternalType internalType) {
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
		} else {
			throw new UnsupportedOperationException("Unsupported type information: "
					+ internalType.toString());
		}
	}

	/**
	 * Get the flink internal type.
	 *
	 * @param fieldSchema hive field schema
	 * @return flink internal type
	 */
	private static InternalType getInternalType(FieldSchema fieldSchema) {
		String type = fieldSchema.getType();
		switch (type) {
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
			case serdeConstants.STRING_TYPE_NAME:
				return StringType.INSTANCE;
			case serdeConstants.CHAR_TYPE_NAME:
				return CharType.INSTANCE;
			case serdeConstants.DATE_TYPE_NAME:
				return DateType.DATE;
			case serdeConstants.DATETIME_TYPE_NAME:
				return TimeType.INSTANCE;
			case serdeConstants.TIMESTAMP_TYPE_NAME:
				return TimestampType.TIMESTAMP;
			case serdeConstants.DECIMAL_TYPE_NAME:
				return DecimalType.DEFAULT;
			default:
				throw new UnsupportedOperationException("Unsupported field schema: name="
						+ fieldSchema.getName() + ", type=" + fieldSchema.getType());
		}
	}

}
