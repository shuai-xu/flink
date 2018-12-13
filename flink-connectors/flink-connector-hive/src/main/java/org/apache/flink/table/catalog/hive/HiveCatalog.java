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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.DatabaseAlreadyExistException;
import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ReadableWritableCatalog;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.parquet.Strings;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_COMPRESSED;
import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_INPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_LOCATION;
import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_NUM_BUCKETS;
import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_OUTPUT_FORMAT;
import static org.apache.flink.table.catalog.hive.HiveCatalogConfig.HIVE_TABLE_SERDE_LIBRARY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Catalog that connects to Hive MetaStore and read/write Hive tables.
 */
public class HiveCatalog implements ReadableWritableCatalog {

	public static final String DEFAULT_FLINK_DATABASE = "default";
	public static final String DEFAULT_FLINK_DATABASE_OWNER = "flink";

	private final String catalogName;
	private final IMetaStoreClient client;

	public HiveCatalog(String catalogName, String hiveMetastoreURI) {
		this(catalogName, getHiveConf(hiveMetastoreURI));
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf)  {
		this(catalogName, getMetastoreClient(hiveConf));
	}

	public HiveCatalog(String catalogName, IMetaStoreClient client) {
		this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
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

	}

	@Override
	public void close() throws IOException {
		client.close();
	}

	// ------ tables ------

	@Override
	public ExternalCatalogTable getTable(ObjectPath tableName) throws TableNotExistException {
		Table hiveTable;

		String dbName = tableName.getDbName();
		String tblName = tableName.getObjectName();
		try {
			hiveTable = client.getTable(dbName, tblName);
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(catalogName, tblName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed getting table %s in database %s", dbName, tblName), e);
		}

		return new ExternalCatalogTable(
			"csv", // TODO: Need a dedicate 'hive' type. Temporarily set to 'csv' for now
			getTableSchema(hiveTable.getSd().getCols()),
			getPropertiesFromHiveTable(hiveTable),
			null,
			null,
			null,
			new LinkedHashSet<>(),
			false,
			null,
			null,
			-1L,
			(long) hiveTable.getCreateTime(),
			(long) hiveTable.getLastAccessTime());
	}

	private Map<String, String> getPropertiesFromHiveTable(Table table) {
		Map<String, String> prop = new HashMap<>(table.getParameters());

		StorageDescriptor sd = table.getSd();

		prop.put(HIVE_TABLE_LOCATION, sd.getLocation());
		prop.put(HIVE_TABLE_SERDE_LIBRARY, sd.getSerdeInfo().getSerializationLib());
		prop.put(HIVE_TABLE_INPUT_FORMAT, sd.getInputFormat());
		prop.put(HIVE_TABLE_OUTPUT_FORMAT, sd.getOutputFormat());
		prop.put(HIVE_TABLE_COMPRESSED, String.valueOf(sd.isCompressed()));
		prop.put(HIVE_TABLE_NUM_BUCKETS, String.valueOf(sd.getNumBuckets()));

		return prop;
	}

	@VisibleForTesting
	protected TableSchema getTableSchema(List<FieldSchema> fieldSchemas) {
		int colSize = fieldSchemas.size();

		String[] colNames = new String[colSize];
		InternalType[] colTypes = new InternalType[colSize];

		for (int i = 0; i < colSize; i++) {
			FieldSchema fs = fieldSchemas.get(i);

			colNames[i] = fs.getName();
			colTypes[i] = TypeConverterUtil.convert(fs.getType());
		}

		return new TableSchema(colNames, colTypes);
	}

	@Override
	public List<ObjectPath> listTablesByDatabase(String dbName) throws DatabaseNotExistException {
		List<String> tables;

		try {
			tables = client.getAllTables(dbName);
		} catch (UnknownDBException e) {
			throw new DatabaseNotExistException(catalogName, dbName);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed listing tables in database %s in HiveCatalog", dbName, catalogName), e);
		}

		return tables.stream()
			.map(t -> new ObjectPath(dbName, t))
			.collect(Collectors.toList());
	}

	@Override
	public List<ObjectPath> listAllTables() {

		List<String> dbs = listDatabases();

		List<ObjectPath> result = new ArrayList<>();

		for (String db : dbs) {
			result.addAll(listTablesByDatabase(db));
		}

		return result;
	}

	@Override
	public void createTable(ObjectPath tableName, ExternalCatalogTable table, boolean ignoreIfExists) throws TableAlreadyExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropTable(ObjectPath tableName, boolean ignoreIfNotExists) throws TableNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tableName, ExternalCatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tableName, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, DatabaseNotExistException {
		throw new UnsupportedOperationException();
	}

	// ------ databases ------

	@Override
	public void createDatabase(String dbName, CatalogDatabase database, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void renameDatabase(String dbName, String newDbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
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

		return new CatalogDatabase();
	}
}
