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

import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DimensionTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.TableProperties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;

/**
 * Hive table factory provides for sql to register hive table.
 */
public class HiveTableFactory implements TableFactory {

	@Override
	public Map<String, String> requiredContext() {
		return null;
	}

	@Override
	public List<String> supportedProperties() {
		return null;
	}

	public TableSource createTableSource(
			String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		String tableName = tableProperties.getString("tableName".toLowerCase(), null);
		String hiveMetastoreURI = tableProperties.getString("hive.metastore.uris".toLowerCase(), null);
		HiveCatalog hiveCatalog = new HiveCatalog(s, hiveMetastoreURI);
		ExternalCatalogTable externalCatalogTable = hiveCatalog.getTable(ObjectPath.fromString(tableName));
		TableStats tableStats = externalCatalogTable.stats();
		HiveConf hiveConf = new HiveConf();
		// We will have to find out some necessary options and expose them to the outside.
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreURI);

		Map<String, String> props = tableProperties.toMap();
		for (Map.Entry<String, String> prop : props.entrySet()) {
			hiveConf.set(prop.getKey(), prop.getValue());
		}
		for (Map.Entry<String, String> prop : externalCatalogTable.properties().entrySet()) {
			hiveConf.set(prop.getKey(), prop.getValue());
		}
		try {
			JobConf jobConf = new JobConf(hiveConf);
			return new HiveTableSource(richTableSchema.getResultTypeInfo(), jobConf, tableStats);
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}

	public DimensionTableSource<?> createDimensionTableSource(
			String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		return null;
	}

	public TableSink<?> createTableSink(
			String s, RichTableSchema richTableSchema, TableProperties tableProperties) {
		return null;
	}
}
