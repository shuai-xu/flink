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

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A catalog implementation holds meta-objects (databases/tables/views/functions) defined by Flink, and
 * leverages Hive Metastore purely as persistent storage.
 */
public class GenericHiveMetastoreCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(GenericHiveMetastoreCatalog.class);

	public GenericHiveMetastoreCatalog(String catalogName, String hiveMetastoreURI) {
		super(catalogName, hiveMetastoreURI);
	}

	public GenericHiveMetastoreCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);
	}

	// ------ tables ------

	@Override
	public CatalogTable getTable(ObjectPath path) throws TableNotExistException {
		Table hiveTable = getHiveTable(path);

		return GenericHiveMetastoreCatalogUtil.createCatalogTable(hiveTable);
	}

	@Override
	public void createTable(ObjectPath path, CatalogTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException {
		try {
			if (tableExists(path)) {
				if (!ignoreIfExists) {
					throw new TableAlreadyExistException(catalogName, path.getFullName());
				}
			} else {
				// Testing shows that createHiveTable() API in Hive 2.3.4 doesn't throw UnknownDBException as it claims
				// Thus we have to manually check if the db exists or not
				if (!dbExists(path.getDbName())) {
					throw new DatabaseNotExistException(catalogName, path.getDbName());
				}

				client.createTable(GenericHiveMetastoreCatalogUtil.createHiveTable(path, table));
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to create table %s", path.getFullName()), e);
		}
	}

	@Override
	public void alterTable(ObjectPath tableName, CatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, DatabaseNotExistException {
		throw new UnsupportedOperationException();
	}

	// ------ table and column stats ------

	@Override
	public TableStats getTableStats(ObjectPath tablePath) throws TableNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableStats(ObjectPath tablePath, TableStats newtTableStats, boolean ignoreIfNotExists) throws TableNotExistException {
		throw new UnsupportedOperationException();
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec, boolean ignoreIfNotExists) throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartition newPartition, boolean ignoreIfNotExists) throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpecs) throws TableNotExistException, TableNotPartitionedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpecs) throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec) {
		throw new UnsupportedOperationException();
	}
}
