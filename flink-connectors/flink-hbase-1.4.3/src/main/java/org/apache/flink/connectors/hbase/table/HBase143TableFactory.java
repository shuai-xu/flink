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
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;

import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_VERSION_VALUE_143;

/**
 * A HBaseTableFactory implementation for HBase version 1.4.3.
 */
public class HBase143TableFactory extends HBaseTableFactoryBase {

	@Override
	String hbaseVersion() {
		return CONNECTOR_VERSION_VALUE_143;
	}

	@Override
	TableSink createTableSink(Map<String, String> properties) {
		preCheck(properties);

		RichTableSchema schema = getTableSchemaFromProperties(properties);

	    // Tuple3 result: (hTableSchema, pkIndex, qualifierColumnIndex)
		Tuple3<HTableSchema, Integer, List<Integer>> bridgeTableInfo = getBridgeTableInfo(properties, schema);

		// HBase143UpsertTableSink can be used for both dataStream and boundedDataStream
		return new HBase143UpsertTableSink(schema, bridgeTableInfo.f0, bridgeTableInfo.f1, bridgeTableInfo.f2, null, properties);
	}

	@Override
	TableSource createTableSource(Map<String, String> properties) {
		preCheck(properties);

		RichTableSchema schema = getTableSchemaFromProperties(properties);

		// Tuple3 result: (hTableSchema, pkIndex, qualifierColumnIndex)
		Tuple3<HTableSchema, Integer, List<Integer>> bridgeTableInfo = getBridgeTableInfo(properties, schema);

		// only lookupable source is valid
		return new HBase143TableSource(schema, bridgeTableInfo.f0, bridgeTableInfo.f1, bridgeTableInfo.f2, properties);
	}
}
