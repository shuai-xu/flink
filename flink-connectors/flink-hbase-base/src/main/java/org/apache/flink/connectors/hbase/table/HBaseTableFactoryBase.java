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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.HTableSchema;
import org.apache.flink.connectors.hbase.util.HTableSchemaUtil;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_COLUMN_FAMILY;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_COLUMN_MAPPING;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_CLIENT_PARAM_PREFIX;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.connectors.hbase.table.HBaseValidator.DEFAULT_COLUMN_FAMILY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;

/**
 * Base class for HBaseTableFactory, the subclass will implement concrete table source/sink creation.
 */
public abstract class HBaseTableFactoryBase
		implements StreamTableSinkFactory<Object>, BatchTableSinkFactory<Row>, StreamTableSourceFactory<Row>, BatchTableSourceFactory<Row> {

	abstract String hbaseVersion();

	abstract TableSink createTableSink(Map<String, String> properties);

	abstract TableSource createTableSource(Map<String, String> properties);

	@Override
	public StreamTableSink<Object> createStreamTableSink(Map<String, String> properties) {
		return (StreamTableSink<Object>) createTableSink(properties);
	}

	@Override
	public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
		return (BatchTableSink<Row>) createTableSink(properties);
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		return (StreamTableSource<Row>) createTableSource(properties);
	}

	@Override
	public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
		return (BatchTableSource<Row>) createTableSource(properties);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE); // hbase
		context.put(CONNECTOR_VERSION, hbaseVersion()); // version
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_HBASE_TABLE_NAME);

		// single column family
		properties.add(CONNECTOR_COLUMN_FAMILY);

		// or use mapping to describe multi column families to a sql table schema
		properties.add(CONNECTOR_COLUMN_MAPPING);

		// HBase's param wildcard
		properties.add(CONNECTOR_HBASE_CLIENT_PARAM_PREFIX);

		// TODO add more support, e.g., async, batch, ...
		return properties;
	}

	protected void preCheck(Map<String, String> properties) {
		String tableName = properties.get(CONNECTOR_HBASE_TABLE_NAME);
		if (StringUtils.isNullOrWhitespaceOnly(tableName)) {
			throw new RuntimeException(CONNECTOR_HBASE_TABLE_NAME + " should not be empty!");
		}
		String hbaseZk = properties.get(CONNECTOR_ZK_QUORUM);
		if (StringUtils.isNullOrWhitespaceOnly(hbaseZk)) {
			Configuration defaultConf = HBaseConfiguration.create();
			String zkQuorum = defaultConf.get(HConstants.ZOOKEEPER_QUORUM);
			if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
				throw new RuntimeException(CONNECTOR_ZK_QUORUM + " should not be empty! " +
					"Pls specify it or ensure an default hbase-site.xml is valid in current class path.");
			}
		}
	}

	protected RichTableSchema getTableSchemaFromProperties(Map<String, String> properties) {
		TableProperties tableProperties = new TableProperties();
		tableProperties.putProperties(properties);
		return tableProperties.readSchemaFromProperties(null);
	}

	/**
	 * Returns Tuple3 result: (hTableSchema, pkIndex, qualifierColumnIndex).
	 */
	protected Tuple3<HTableSchema, Integer, List<Integer>> getBridgeTableInfo(Map<String, String> properties, RichTableSchema schema) {
		String tableName = properties.get(CONNECTOR_HBASE_TABLE_NAME);

		// apply sequence: columnMapping > columnFamily > DEFAULT_COLUMN_FAMILY
		String columnFamily = properties.get(CONNECTOR_COLUMN_FAMILY);
		String columnMapping = properties.get(CONNECTOR_COLUMN_MAPPING);

		Tuple3<HTableSchema, Integer, List<Integer>> bridgeTableInfo;
		if (!StringUtils.isNullOrWhitespaceOnly(columnMapping)) {
			// using columnMapping for multi column families case
			bridgeTableInfo = HTableSchemaUtil.fromRichTableSchemaWithColumnMapping(tableName, schema, columnMapping);
		} else {
			// using columnFamily or default column family name for single column families case
			String finalCF = StringUtils.isNullOrWhitespaceOnly(columnFamily) ? DEFAULT_COLUMN_FAMILY : columnFamily;
			// construct a HTableSchema
			bridgeTableInfo = HTableSchemaUtil.fromRichTableSchemaWithSingleColumnFamily(tableName, schema, finalCF);
		}
		return bridgeTableInfo;
	}
}
