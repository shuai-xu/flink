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

package org.apache.flink.streaming.connectors.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.catalog.FlinkCatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalogUtil;
import org.apache.flink.table.catalog.hive.config.HiveMetastoreConfig;
import org.apache.flink.table.catalog.hive.config.HiveTableConfig;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.TableSourceParserFactory;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.types.InternalType;
import org.apache.flink.table.types.TypeConverters;
import org.apache.flink.table.util.TableProperties;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.DEFAULT_LIST_COLUMN_TYPES_SEPARATOR;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_DB_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_NAMES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_FIELD_TYPES;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TABLE_NAME;
import static org.apache.flink.table.catalog.hive.config.HiveTableConfig.HIVE_TABLE_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

/**
 * Hive table factory provides for sql to register hive table.
 */
public class HiveTableFactory implements BatchTableSourceFactory<BaseRow>, TableSourceParserFactory,
										BatchTableSinkFactory<BaseRow> {
	private static Logger logger = LoggerFactory.getLogger(HiveTableFactory.class);
	/**
	 * Add this method to make some optimize rule work normally, we should delete it later.
	 */
	@Override
	public TableSourceParser createParser(
			String tableName, RichTableSchema tableSchema, TableProperties properties) {
		return null;
	}

	@Override
	public BatchTableSource<BaseRow> createBatchTableSource(Map<String, String> props) {
		HiveTableInfo hiveTableInfo = new HiveTableInfo(props);
		try {
			return new HiveTableSource(new RowTypeInfo(hiveTableInfo.getTypeInformations(), hiveTableInfo.getFieldNames()),
									hiveTableInfo.getHiveRowTypeString(),
									new JobConf(hiveTableInfo.getHiveConf()),
									hiveTableInfo.getTableStats(),
									hiveTableInfo.getHiveDbName(),
									hiveTableInfo.getHiveTableName(),
									hiveTableInfo.getPartitionColumns());
		} catch (Exception e){
			logger.error("Error when create hive batch table source ...", e);
			throw new FlinkCatalogException(e);
		}
	}

	@Override
	public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> properties) {
		HiveTableInfo hiveTableInfo = new HiveTableInfo(properties);
		// todo: solve dynamic partition table write problem
		return new HiveTableSink(new JobConf(hiveTableInfo.getHiveConf()),
								new RowTypeInfo(hiveTableInfo.getTypeInformations(),
								hiveTableInfo.getFieldNames()),
								hiveTableInfo.getHiveDbName(),
								hiveTableInfo.getHiveTableName(),
								hiveTableInfo.getPartitionColumns(),
								null);
	}

	@Override
	public Map<String, String> requiredContext() {
		HashMap<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, "HIVE");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_PROPERTY_VERSION);
		properties.add(HIVE_TABLE_TYPE);

		// Hive catalog configs
		properties.add(HiveMetastoreConfig.HIVE_METASTORE_USERNAME);

		// Hive table configs
		properties.add(HiveTableConfig.HIVE_TABLE_COMPRESSED);
		properties.add(HiveTableConfig.HIVE_TABLE_INPUT_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_LOCATION);
		properties.add(HiveTableConfig.HIVE_TABLE_NUM_BUCKETS);
		properties.add(HiveTableConfig.HIVE_TABLE_OUTPUT_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_SERDE_LIBRARY);
		properties.add(HiveTableConfig.HIVE_TABLE_STORAGE_SERIALIZATION_FORMAT);
		properties.add(HiveTableConfig.HIVE_TABLE_FIELD_NAMES);
		properties.add(HiveTableConfig.HIVE_TABLE_FIELD_TYPES);

		// Hive table parameters
		properties.add(HiveTableConfig.HIVE_TABLE_PROPERTY_TRANSIENT_LASTDDLTIME);
		properties.add(HiveTableConfig.HIVE_TABLE_PROPERTY_LAST_MODIFIED_TIME);

		// Hive table stats
		properties.add(StatsSetupConst.NUM_FILES);
		properties.add(StatsSetupConst.NUM_PARTITIONS);
		properties.add(StatsSetupConst.TOTAL_SIZE);
		properties.add(StatsSetupConst.RAW_DATA_SIZE);
		properties.add(StatsSetupConst.ROW_COUNT);
		properties.add(StatsSetupConst.COLUMN_STATS_ACCURATE);

		properties.add(HiveTableConfig.HIVE_TABLE_DB_NAME);
		properties.add(HiveTableConfig.HIVE_TABLE_TABLE_NAME);
		properties.add(HiveTableConfig.HIVE_TABLE_PARTITION_FIELDS);
		properties.add(HiveConf.ConfVars.METASTOREURIS.varname);

		return properties;
	}

	private class HiveTableInfo {
		private Map<String, String> props;
		private HiveConf hiveConf;
		private TableStats tableStats;
		private String[] fieldNames;
		private String hiveRowTypeString;
		private TypeInformation[] typeInformations;
		private String hiveDbName;
		private String hiveTableName;
		private String[] partitionColumns;

		public HiveTableInfo(Map<String, String> props) {
			this.props = props;
			build();
		}

		public HiveConf getHiveConf() {
			return hiveConf;
		}

		public TableStats getTableStats() {
			return tableStats;
		}

		public String[] getFieldNames() {
			return fieldNames;
		}

		public String getHiveRowTypeString() {
			return hiveRowTypeString;
		}

		public TypeInformation[] getTypeInformations() {
			return typeInformations;
		}

		public String getHiveDbName() {
			return hiveDbName;
		}

		public String getHiveTableName() {
			return hiveTableName;
		}

		public String[] getPartitionColumns() {
			return partitionColumns;
		}

		private HiveTableInfo build() {
			hiveConf = new HiveConf();
			tableStats = null;
			for (Map.Entry<String, String> prop : props.entrySet()) {
				hiveConf.set(prop.getKey(), prop.getValue());
			}
			fieldNames = props.get(HIVE_TABLE_FIELD_NAMES).split(",");
			hiveRowTypeString = props.get(HIVE_TABLE_FIELD_TYPES);
			String[] hiveFieldTypes = hiveRowTypeString.split(DEFAULT_LIST_COLUMN_TYPES_SEPARATOR);
			InternalType[] colTypes = new InternalType[fieldNames.length];
			typeInformations = new TypeInformation[fieldNames.length];
			for (int i = 0; i < hiveFieldTypes.length; i++) {
				colTypes[i] = HiveCatalogUtil.convert(hiveFieldTypes[i]);
				typeInformations[i] = TypeConverters.createExternalTypeInfoFromDataType(colTypes[i]);
			}
			hiveDbName = props.get(HIVE_TABLE_DB_NAME);
			hiveTableName = props.get(HIVE_TABLE_TABLE_NAME);
			String partitionFields = props.get(HIVE_TABLE_PARTITION_FIELDS);
			partitionColumns = new String[0];
			if (null != partitionFields && !partitionFields.isEmpty()) {
				partitionColumns = partitionFields.split(",");
			}
			hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, props.get(HiveConf.ConfVars.METASTOREURIS.varname));
			return this;
		}
	}
}
