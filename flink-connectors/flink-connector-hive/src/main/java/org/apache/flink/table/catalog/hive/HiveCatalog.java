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

import org.apache.flink.hive.shaded.org.apache.thrift.TException;
import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.parquet.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog that connects to Hive MetaStore and read/write Hive tables.
 */
public class HiveCatalog implements ReadableWritableCatalog {

	private final String catalogName;

	private HiveConf hiveConf;
	private IMetaStoreClient client;

	public HiveCatalog(String catalogName, String hiveMetastoreURI) {
		this(catalogName, getHiveConf(hiveMetastoreURI));
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf)  {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		this.catalogName = catalogName;

		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
	}

	public HiveCatalog(String catalogName, IMetaStoreClient client) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		this.catalogName = catalogName;

		this.client = checkNotNull(client, "client cannot be null");
	}

	private static HiveConf getHiveConf(String hiveMetastoreURI) {
		checkArgument(!Strings.isNullOrEmpty(hiveMetastoreURI), "hiveMetastoreURI cannot be null or empty");

		HiveConf hiveConf = new HiveConf();
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreURI);
		return hiveConf;
	}

	private static IMetaStoreClient getMetastoreClient(HiveConf hiveConf) {
		try {
			return RetryingMetaStoreClient.getProxy(
				hiveConf,
				null,
				null,
				HiveMetaStoreClient.class.getName(),
				true);
		} catch (MetaException e) {
			throw new FlinkHiveException("Failed creating Hive metastore client", e);
		}
	}

	@Override
	public void open() {
		if (client == null) {
			client = getMetastoreClient(hiveConf);
		}
	}

	@Override
	public void close() {
		if (client != null) {
			client.close();
			client = null;
		}
	}

	// ------ tables ------

	@Override
	public ExternalCatalogTable getTable(ObjectPath path) throws TableNotExistException {
		return HiveMetadataUtil.createExternalCatalogTable(getHiveTable(path));
	}

	private Table getHiveTable(ObjectPath path) {
		try {
			return client.getTable(path.getDbName(), path.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, path.getFullName());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed getting table %s", path.getFullName()), e);
		}
	}

	@Override
	public List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException {
		try {
			return client.getAllTables(dbName).stream()
				.map(t -> new ObjectPath(dbName, t))
				.collect(Collectors.toList());
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing tables in database %s", dbName), e);
		}
	}

	@Override
	public List<ObjectPath> listAllTables() {
		List<String> dbs = listDatabases();
		List<ObjectPath> result = new ArrayList<>();

		for (String db : dbs) {
			result.addAll(listTables(db));
		}

		return result;
	}

	@Override
	public void createTable(ObjectPath path, ExternalCatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

		try {
			if (tableExists(path)) {
				if (!ignoreIfExists) {
					throw new TableAlreadyExistException(catalogName, path.getFullName());
				}
			} else {
				// Testing shows that createTable() API in Hive 2.3.4 doesn't throw UnknownDBException as it claims
				// Thus we have to manually check if the db exists or not
				if (!dbExists(path.getDbName())) {
					throw new DatabaseNotExistException(catalogName, path.getDbName());
				}

				client.createTable(HiveMetadataUtil.createHiveTable(path, table));
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed creating table %s", path.getFullName()), e);
		}
	}

	@Override
	public void dropTable(ObjectPath path, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			client.dropTable(path.getDbName(), path.getObjectName(), true, ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed dropping table %s", path.getFullName()), e);
		}
	}

	@Override
	public void alterTable(ObjectPath path, ExternalCatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			if (tableExists(path)) {
				client.alter_table(path.getDbName(), path.getObjectName(), HiveMetadataUtil.createHiveTable(path, newTable));
			} else if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed creating table %s", path.getFullName()), e);
		}
	}

	@Override
	public void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, DatabaseNotExistException {
		// Hive metastore client doesn't support renaming yet
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tableExists(ObjectPath path) {
		try {
			return client.tableExists(path.getDbName(), path.getObjectName());
		} catch (UnknownDBException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed check if table %s exists in database %s", path.getObjectName(), path.getDbName()), e);
		}
	}

	// ------ databases ------

	@Override
	public void createDatabase(String dbName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		try {
			client.createDatabase(HiveMetadataUtil.createHiveDatabase(dbName, db));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed creating database %s", dbName), e);
		}
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		try {
			client.dropDatabase(dbName, true, ignoreIfNotExists);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed dropping database %s", dbName), e);
		}
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		try {
			if (dbExists(dbName)) {
				client.alterDatabase(dbName, HiveMetadataUtil.createHiveDatabase(dbName, newDatabase));
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed altering database %s", dbName), e);
		}
	}

	@Override
	public void renameDatabase(String dbName, String newDbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		// Hive metastore client doesn't support renaming yet
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> listDatabases() {
		try {
			return client.getAllDatabases();
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing all databases in HiveCatalog %s", catalogName));
		}
	}

	@Override
	public CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException {
		Database hiveDb;

		try {
			hiveDb = client.getDatabase(dbName);
		} catch (NoSuchObjectException e) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed getting database %s from HiveCatalog %s", dbName, catalogName), e);
		}

		// TODO: Transform hiveDb to CatalogDatabase

		return HiveMetadataUtil.createCatalogDatabase(hiveDb);
	}

	@Override
	public boolean dbExists(String dbName) {
		try {
			return client.getDatabase(dbName) != null;
		} catch (NoSuchObjectException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed getting database %s", dbName), e);
		}
	}

	// ------ partitions ------

	@Override
	public void createParition(ObjectPath tablePath, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropParition(ObjectPath tablePath, CatalogPartition.PartitionSpec partition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterParition(ObjectPath tablePath, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path)
		throws TableNotExistException, TableNotPartitionedException {

		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), (short) -1).stream()
				.map(n -> HiveMetadataUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing partitions of table %s", path), e);
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path, CatalogPartition.PartitionSpec partitionSpecs)
		throws TableNotExistException, TableNotPartitionedException {

		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(path, partitionSpecs), (short) -1).stream()
				.map(n -> HiveMetadataUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing partitions of table %s", path), e);
		}
	}

	@Override
	public CatalogPartition getPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpecs)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		if (!isTablePartitioned(path)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return HiveMetadataUtil.createCatalogPartition(
				partitionSpecs,
				client.getPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(path, partitionSpecs)));
		} catch (NoSuchObjectException e) {
			throw new PartitionNotExistException(catalogName, path, partitionSpecs);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing partitions of table %s", path), e);
		}
	}

	private List<String> getOrderedPartitionValues(ObjectPath tablePath, CatalogPartition.PartitionSpec partitionSpec) {
		return partitionSpec.getOrderedValues(getPartitionKeys(tablePath));
	}

	private List<String> getPartitionKeys(ObjectPath tablePath) {
		List<FieldSchema> fieldSchemas = getHiveTable(tablePath).getPartitionKeys();

		List<String> partitionKeys = new ArrayList<>(fieldSchemas.size());

		for (FieldSchema fs : fieldSchemas) {
			partitionKeys.add(fs.getName());
		}

		return partitionKeys;
	}
}
