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

import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.parquet.Strings;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for Hive-metastore backed catalogs.
 */
public abstract class HiveCatalogBase implements ReadableWritableCatalog {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogBase.class);

	private static final String DEFAULT_DB = "default";

	protected final String catalogName;

	private String defaultDatabaseName = DEFAULT_DB;
	protected HiveConf hiveConf;
	protected IMetaStoreClient client;

	public HiveCatalogBase(String catalogName, String hiveMetastoreURI) {
		this(catalogName, getHiveConf(hiveMetastoreURI));
	}

	public HiveCatalogBase(String catalogName, HiveConf hiveConf) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(catalogName), "catalogName cannot be null or empty");
		this.catalogName = catalogName;

		this.hiveConf = checkNotNull(hiveConf, "hiveConf cannot be null");
	}

	private static HiveConf getHiveConf(String hiveMetastoreURI) {
		checkArgument(!Strings.isNullOrEmpty(hiveMetastoreURI), "hiveMetastoreURI cannot be null or empty");

		HiveConf hiveConf = new HiveConf();
		hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetastoreURI);
		return hiveConf;
	}

	protected static IMetaStoreClient getMetastoreClient(HiveConf hiveConf) {
		try {
			return RetryingMetaStoreClient.getProxy(
				hiveConf,
				null,
				null,
				HiveMetaStoreClient.class.getName(),
				true);
		} catch (MetaException e) {
			throw new FlinkHiveException("Failed to create Hive metastore client", e);
		}
	}

	@Override
	public String getDefaultDatabaseName() {
		return defaultDatabaseName;
	}

	@Override
	public void setDefaultDatabaseName(String databaseName) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

		defaultDatabaseName = databaseName;
	}

	@Override
	public void open() {
		if (client == null) {
			client = getMetastoreClient(hiveConf);
			LOG.info("Connect to Hive metastore");
		}
	}

	@Override
	public void close() {
		if (client != null) {
			client.close();
			client = null;
			LOG.info("Close connection to Hive metastore");
		}
	}

	protected Table getHiveTable(ObjectPath path) throws TableNotExistException {
		try {
			return client.getTable(path.getDbName(), path.getObjectName());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, path.getFullName());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get table %s", path.getFullName()), e);
		}
	}

	protected Long getRowCount(Table hiveTable) {
		if (hiveTable.getParameters().get(StatsSetupConst.ROW_COUNT) != null) {
			return Math.max(0L, Long.parseLong(hiveTable.getParameters().get(StatsSetupConst.ROW_COUNT)));
		} else {
			return 0L;
		}
	}

	// ------ tables and views------

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
				String.format("Failed to drop table %s", path.getFullName()), e);
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
				String.format("Failed to list tables in database %s", dbName), e);
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
	public boolean tableExists(ObjectPath path) {
		try {
			return client.tableExists(path.getDbName(), path.getObjectName());
		} catch (UnknownDBException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to check if table %s exists in database %s", path.getObjectName(), path.getDbName()), e);
		}
	}

	// ------ databases ------

	/**
	 * Create a Hive database from CatalogDatabase.
	 *
	 * @param dbName	Name of the database
	 * @param db		The given CatalogDatabase
	 * @return A Hive Database
	 */
	protected abstract Database createHiveDatabase(String dbName, CatalogDatabase db);

	/**
	 * Create a Hive database from CatalogDatabase.
	 *
	 * @param hiveDb	The given Hive Database
	 * @return	A CatalogDatabase
	 */
	protected abstract CatalogDatabase createCatalogDatabase(Database hiveDb);

	@Override
	public void createDatabase(String dbName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		try {
			client.createDatabase(createHiveDatabase(dbName, db));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new DatabaseAlreadyExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed to create database %s", dbName), e);
		}
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		try {
			if (dbExists(dbName)) {
				client.alterDatabase(dbName, createHiveDatabase(dbName, newDatabase));
			} else if (!ignoreIfNotExists) {
				throw new DatabaseNotExistException(catalogName, dbName);
			}
		} catch (TException e) {
			throw new FlinkHiveException(String.format("Failed to alter database %s", dbName), e);
		}
	}

	@Override
	public void renameDatabase(String dbName, String newDbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		// Hive metastore client doesn't support renaming yet
		throw new UnsupportedOperationException();
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
				String.format("Failed to get database %s from HiveCatalog %s", dbName, catalogName), e);
		}

		return createCatalogDatabase(hiveDb);
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
			throw new FlinkHiveException(String.format("Failed to drop database %s", dbName), e);
		}
	}

	@Override
	public List<String> listDatabases() {
		try {
			return client.getAllDatabases();
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list all databases in HiveCatalog %s", catalogName));
		}
	}

	@Override
	public boolean dbExists(String dbName) {
		try {
			return client.getDatabase(dbName) != null;
		} catch (NoSuchObjectException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get database %s", dbName), e);
		}
	}
}
