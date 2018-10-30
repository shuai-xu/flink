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

import org.apache.flink.connectors.csv.CsvTableFactory;
import org.apache.flink.connectors.csv.RetractCsvTableFactory;
import org.apache.flink.connectors.csv.UpsertCsvTableFactory;
import org.apache.flink.connectors.orc.OrcTableFactory;
import org.apache.flink.connectors.parquet.ParquetTableFactory;
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
		DIRECTORY.put("CSV", CsvTableFactory.class.getCanonicalName());
		DIRECTORY.put("UPSERTCSV", UpsertCsvTableFactory.class.getCanonicalName());
		DIRECTORY.put("RETRACTCSV", RetractCsvTableFactory.class.getCanonicalName());
		DIRECTORY.put("PARQUET", ParquetTableFactory.class.getCanonicalName());
		DIRECTORY.put("ADS", "com.alibaba.blink.connectors.ads.AdsTableFactory");
		DIRECTORY.put("ALIHBASE", "com.alibaba.blink.streaming.connector.alihbase094.AliHBase094TableFactory");
		DIRECTORY.put("ALIHBASE11", "com.alibaba.blink.streaming.connector.alihbase11.AliHBase11TableFactory");
		DIRECTORY.put("CUSTOM", "com.alibaba.blink.streaming.connector.custom.CustomTableFactory");
		DIRECTORY.put("DATAHUB", "com.alibaba.blink.streaming.connectors.datahub.DataHubTableFactory");
		DIRECTORY.put("HQUEUE", "com.alibaba.blink.connectors.hqueue.HQueueTableFactory");
		DIRECTORY.put("HITSDB", "com.alibaba.blink.connectors.hitsdb.HiTSDBTableFactory");
		DIRECTORY.put("METAQ", "com.alibaba.blink.streaming.connector.metaq.MetaQTableFactory");
		DIRECTORY.put("MQ", "com.alibaba.blink.streaming.connector.metaq.MQTableFactory");
		DIRECTORY.put("NOTIFY", "com.alibaba.blink.streaming.connector.notify.NotifyTableFactory");
		DIRECTORY.put("OCEANBASE", "com.alibaba.blink.streaming.connector.oceanbase.OceanBaseTableFactory");
		DIRECTORY.put("OTS", "com.alibaba.blink.connectors.ots.OtsTableFactory");
		DIRECTORY.put("PRINT", "com.alibaba.blink.connectors.print.PrintTableFactory");
		DIRECTORY.put("RANDOM", "com.alibaba.blink.streaming.connector.random.RandomTableFactory");
		DIRECTORY.put("RDS", "com.alibaba.blink.connectors.rds.RdsTableFactory");
		DIRECTORY.put("SLS", "com.alibaba.blink.streaming.connector.sls.SlsTableFactory");
		DIRECTORY.put("TT", "com.alibaba.blink.streaming.connectors.tt.TT4TableFactory");
		DIRECTORY.put("TDDL", "com.alibaba.blink.connectors.tddl.TddlTableFactory");
		DIRECTORY.put("SWIFT", "com.alibaba.blink.streaming.connector.swift.SwiftTableFactory");
		DIRECTORY.put("PETADATA", "com.alibaba.blink.connectors.petadata.PetaTableFactory");
		DIRECTORY.put("HYBRIDDB", "com.alibaba.blink.connectors.petadata.PetaTableFactory");
		DIRECTORY.put("IGRAPH", "com.alibaba.blink.streaming.connectors.igraph.IGraphTableFactory");
		DIRECTORY.put("TAIR", "com.alibaba.blink.streaming.connectors.tair.TairTableFactory");
		DIRECTORY.put("HFILE", "com.alibaba.blink.batch.connector.hfile.HFileTableFactory");
		DIRECTORY.put("ODPS", "com.alibaba.blink.connectors.odps.OdpsTableFactory");
		DIRECTORY.put("HBASESCAN", "com.alibaba.blink.streaming.connector.scan.hbase.HBaseScanTableFactory");
		DIRECTORY.put("HDFS", "com.alibaba.blink.streaming.connectors.hdfs.HDFSTableFactory");
		DIRECTORY.put("HBASE", "com.alibaba.blink.streaming.connector.hbase.HBaseTableFactory");
		DIRECTORY.put("DRC", "com.alibaba.blink.streaming.connectors.drc.DRCTableFactory");
		DIRECTORY.put("LINDORM", "com.alibaba.blink.streaming.connector.lindorm.LindormTableFactory");
		DIRECTORY.put("CLOUDHBASE", "com.alibaba.blink.streaming.connector.cloudHbase.CloudHbaseTableFactory");
		DIRECTORY.put("MYSQLSCAN", "com.alibaba.blink.connectors.mysql.scan.MySQLScanTableFactory");
		DIRECTORY.put("ELASTICSEARCH", "com.alibaba.blink.streaming.connectors.elasticsearch.ElasticSearchTableFactory");
		DIRECTORY.put("KAFKA08", "com.alibaba.blink.streaming.connectors.kafka08.Kafka08TableFactory");
		DIRECTORY.put("KAFKA09", "com.alibaba.blink.streaming.connectors.kafka09.Kafka09TableFactory");
		DIRECTORY.put("KAFKA010", "com.alibaba.blink.streaming.connectors.kafka010.Kafka010TableFactory");
		DIRECTORY.put("KAFKA011", "com.alibaba.blink.streaming.connectors.kafka011.Kafka011TableFactory");
		DIRECTORY.put("JDBC", "com.alibaba.blink.connectors.jdbc.JdbcTableFactory");
		DIRECTORY.put("ORC", OrcTableFactory.class.getCanonicalName());
		DIRECTORY.put("HADOOPCOMPATIBILITY", "com.alibaba.blink.connectors.hadoopcompatibility.HadoopCompatibilityTableFactory");
		DIRECTORY.put("BLINKSTORE", "com.alibaba.blink.connectors.store.BlinkStoreTableFactory");
		DIRECTORY.put("REDIS", "com.alibaba.blink.connector.redis.RedisTableFactory");
		DIRECTORY.put("RMQ", "org.apache.flink.streaming.connectors.rabbitmq.table.RMQTableFactory");
		// For IT tests
		DIRECTORY.put("TESTSOURCE", "com.alibaba.blink.launcher.autoconfig.connector.DummyTableFactory");
		DIRECTORY.put("TESTSINK", "com.alibaba.blink.launcher.autoconfig.connector.PrintTableFactory");
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
