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

package org.apache.flink.factories;

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableFactory;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 *
 */
public class FlinkTableFactory implements TableFactory {

	private ClassLoader classLoader;

	public static final FlinkTableFactory INSTANCE = new FlinkTableFactory();

	/**
	 * Data structure mapping storage type to the corresponding implementations.
	 */
	public static final Map<String, String> DIRECTORY = new HashMap<>();

	static {
		DIRECTORY.put("CSV", "org.apache.flink.connectors.csv.CsvTableFactory");
		DIRECTORY.put("UPSERTCSV", "org.apache.flink.connectors.csv.UpsertCsvTableFactory");
		DIRECTORY.put("RETRACTCSV", "org.apache.flink.connectors.csv.RetractCsvTableFactory");
		DIRECTORY.put("PARQUET", "org.apache.flink.connectors.parquet.ParquetTableFactory");
		DIRECTORY.put("CUSTOM", "org.apache.flink.streaming.connector.custom.CustomTableFactory");
		DIRECTORY.put("DATAHUB", "org.apache.flink.streaming.connectors.datahub.DataHubTableFactory");
		DIRECTORY.put("RANDOM", "org.apache.flink.streaming.connector.random.RandomTableFactory");
		DIRECTORY.put("TAIR", "org.apache.flink.streaming.connectors.tair.TairTableFactory");
		DIRECTORY.put("HFILE", "org.apache.flink.batch.connector.hfile.HFileTableFactory");
		DIRECTORY.put("ODPS", "org.apache.flink.connectors.odps.OdpsTableFactory");
		DIRECTORY.put("HBASESCAN", "org.apache.flink.streaming.connector.scan.hbase.HBaseScanTableFactory");
		DIRECTORY.put("HDFS", "org.apache.flink.streaming.connectors.hdfs.HDFSTableFactory");
		DIRECTORY.put("HBASE", "org.apache.flink.streaming.connector.hbase.HBaseTableFactory");
		DIRECTORY.put("CLOUDHBASE", "org.apache.flink.streaming.connector.cloudHbase.CloudHbaseTableFactory");
		DIRECTORY.put("MYSQLSCAN", "org.apache.flink.connectors.mysql.scan.MySQLScanTableFactory");
		DIRECTORY.put("ELASTICSEARCH", "org.apache.flink.streaming.connectors.elasticsearch.ElasticSearchTableFactory");
		DIRECTORY.put("KAFKA08", "org.apache.flink.streaming.connectors.kafka08.Kafka08TableFactory");
		DIRECTORY.put("KAFKA09", "org.apache.flink.streaming.connectors.kafka09.Kafka09TableFactory");
		DIRECTORY.put("KAFKA010", "org.apache.flink.streaming.connectors.kafka010.Kafka010TableFactory");
		DIRECTORY.put("KAFKA011", "org.apache.flink.streaming.connectors.kafka011.Kafka011TableFactory");
		DIRECTORY.put("JDBC", "org.apache.flink.connectors.jdbc.JdbcTableFactory");
		DIRECTORY.put("ORC", "org.apache.flink.connectors.orc.OrcTableFactory");
		DIRECTORY.put("HADOOPCOMPATIBILITY", "org.apache.flink.connectors.hadoopcompatibility.HadoopCompatibilityTableFactory");
		DIRECTORY.put("BLINKSTORE", "org.apache.flink.connectors.store.BlinkStoreTableFactory");
		DIRECTORY.put("REDIS", "org.apache.flink.connector.redis.RedisTableFactory");
		// For IT tests
		DIRECTORY.put("TESTSOURCE", "org.apache.flink.launcher.autoconf.DummyTableFactory");
		DIRECTORY.put("TESTSINK", "org.apache.flink.launcher.autoconf.copy.PrintTableFactory");
	}

	@Override
	public TableSource createTableSource(
		String tableName,
		RichTableSchema schema,
		TableProperties properties) {
		TableFactory factory = getTableFactory(tableName, schema, properties);
		return factory.createTableSource(tableName, schema, properties.toKeyLowerCase());
	}

	@Override
	public TableSourceParser createParser(
		String tableName,
		RichTableSchema schema,
		TableProperties properties) {
		TableFactory factory = getTableFactory(tableName, schema, properties);
		return factory.createParser(tableName, schema, properties.toKeyLowerCase());
	}

	@Override
	public DimensionTableSource createDimensionTableSource(
		String tableName,
		RichTableSchema schema,
		TableProperties properties) {
		TableFactory factory = getTableFactory(tableName, schema, properties);
		return factory.createDimensionTableSource(tableName, schema, properties.toKeyLowerCase());
	}

	@Override
	public TableSink createTableSink(
		String tableName,
		RichTableSchema schema,
		TableProperties properties) {
		TableFactory factory = getTableFactory(tableName, schema, properties);
		return factory.createTableSink(tableName, schema, properties.toKeyLowerCase());
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------
	private TableFactory getTableFactory(String tableName, RichTableSchema schema, TableProperties properties) {
		requireNonNull(schema, "schema must not be null");
		requireNonNull(properties, "table properties must not be null");

		String type = getStorageType(tableName, properties);
		String clazz = DIRECTORY.get(type);
		if (StringUtils.isEmpty(clazz)) {
			throw new UnsupportedOperationException(type + "插件暂不支持.");
		}
		TableFactory factory = instantiateTableFactory(clazz);
		factory.setClassLoader(classLoader);
		return factory;
	}

	private static Class<? extends TableFactory> getClassByName(String className) throws ClassNotFoundException {
		return Class.forName(
			className,
			true,
			Thread.currentThread().getContextClassLoader())
			.asSubclass(TableFactory.class);
	}

	private static TableFactory instantiateTableFactory(String className) {
		try {
			Class<? extends TableFactory> tfClass = getClassByName(className);
			return tfClass.newInstance();
		} catch (ClassNotFoundException e) {
			throw new TableException("Could not load table factory class '" + className + '\'', e);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new TableException("Could not instantiate table factory class: " + e.getMessage(), e);
		}
	}

	private static String getStorageType(String tableName, TableProperties properties) {
		String type = properties.getString("type", null);
		if (type != null) {
			return type.toUpperCase();
		} else {
			throw new IllegalArgumentException("Property 'type' of table " + tableName + " is missing");
		}
	}

	@Override
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
