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
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.FlinkInMemoryCatalog;
import org.apache.flink.table.catalog.FlinkTempFunction;
import org.apache.flink.table.catalog.FlinkTempTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A catalog implementation holds meta-objects (databases/tables/views/functions) defined by Flink, and
 * leverages Hive Metastore purely as persistent storage.
 */
public class GenericHiveMetastoreCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(GenericHiveMetastoreCatalog.class);

	// This only serves to fill the in-memory catalog with a name, and should not be used anywhere else
	private static final String BUILTIN_CATALOG_NAME = "generic-hive-metastore-builtin";

	private final FlinkInMemoryCatalog inMemoryCatalog;

	public GenericHiveMetastoreCatalog(String catalogName, String hiveMetastoreURI) {
		super(catalogName, hiveMetastoreURI);

		inMemoryCatalog = new FlinkInMemoryCatalog(BUILTIN_CATALOG_NAME);
		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	public GenericHiveMetastoreCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);

		inMemoryCatalog = new FlinkInMemoryCatalog(BUILTIN_CATALOG_NAME);
		LOG.info("Created GenericHiveMetastoreCatalog '{}'", catalogName);
	}

	// ------ tables and views ------

	@Override
	public CatalogTable getTable(ObjectPath path) throws TableNotExistException {
		if (inMemoryCatalog.tableExists(path)) {
			return inMemoryCatalog.getTable(path);
		} else {
			Table hiveTable = getHiveTable(path);

			return GenericHiveMetastoreCatalogUtil.createCatalogTable(hiveTable);
		}
	}

	@Override
	public void dropTable(ObjectPath path, boolean ignoreIfNotExists) throws TableNotExistException {
		if (inMemoryCatalog.tableExists(path)) {
			inMemoryCatalog.dropTable(path, ignoreIfNotExists);
		} else {
			super.dropTable(path, ignoreIfNotExists);
		}
	}

	@Override
	public List<ObjectPath> listTables(String dbName) throws DatabaseNotExistException {
		List<ObjectPath> result = new ArrayList<>();
		result.addAll(inMemoryCatalog.listTables(dbName));
		result.addAll(super.listTables(dbName));

		return result;
	}

	@Override
	public List<ObjectPath> listAllTables() {
		List<ObjectPath> result = new ArrayList<>();
		result.addAll(inMemoryCatalog.listAllTables());
		result.addAll(super.listAllTables());

		return result;
	}

	@Override
	public boolean tableExists(ObjectPath path) {
		return inMemoryCatalog.tableExists(path) || super.tableExists(path);
	}

	// ------ tables ------

	@Override
	public void createTable(ObjectPath path, CatalogTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException {
		// Check no table exists with such name in either in-memory or hive
		if (tableExists(path)) {
			if (!ignoreIfExists) {
				throw new TableAlreadyExistException(catalogName, path.getFullName());
			}
		} else {
			if (table instanceof FlinkTempTable) {
				inMemoryCatalog.createTable(path, table, ignoreIfExists);
			} else {
				try {
					// Testing shows that createHiveTable() API in Hive 2.3.4 doesn't throw UnknownDBException as it claims
					// Thus we have to manually check if the db exists or not
					if (!dbExists(path.getDbName())) {
						throw new DatabaseNotExistException(catalogName, path.getDbName());
					}
					client.createTable(GenericHiveMetastoreCatalogUtil.createHiveTable(path, table));
				} catch (TException e) {
					throw new FlinkHiveException(String.format("Failed to create table %s", path.getFullName()), e);
				}
			}
		}
	}

	@Override
	public void alterTable(ObjectPath path, CatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		if (!tableExists(path)) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} else {
			if (inMemoryCatalog.tableExists(path)) {
				if (newTable instanceof FlinkTempTable) {
					inMemoryCatalog.alterTable(path, newTable, ignoreIfNotExists);
				} else {
					throw new IllegalArgumentException(
						String.format("Table %s is a FlinkTempTable, the new table is a CatalogTable.", path.getFullName()));
				}
			} else {
				if (!(newTable instanceof FlinkTempTable)) {
					// IMetastoreClient.alter_table() requires the table to have a valid location, which it doesn't in this case
					// Thus we have to translate alterTable() into (dropTable() + createTable())
					dropTable(path, false);
					createTable(path, newTable, false);
				} else {
					throw new IllegalArgumentException(
						String.format("Table %s is a CatalogTable, the new table is a FlinkTempTable.", path.getFullName()));
				}
			}
		}
	}

	// ------ views ------

	@Override
	public void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists) {
		throw new UnsupportedOperationException();
	}

	// ------ databases ------

	@Override
	public void createDatabase(String dbName, CatalogDatabase db, boolean ignoreIfExists) throws DatabaseAlreadyExistException {
		inMemoryCatalog.createDatabase(dbName, db, ignoreIfExists);
		super.createDatabase(dbName, db, ignoreIfExists);
	}

	@Override
	public void alterDatabase(String dbName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		inMemoryCatalog.alterDatabase(dbName, newDatabase, ignoreIfNotExists);
		super.alterDatabase(dbName, newDatabase, ignoreIfNotExists);
	}

	@Override
	public void dropDatabase(String dbName, boolean ignoreIfNotExists) throws DatabaseNotExistException {
		inMemoryCatalog.dropDatabase(dbName, ignoreIfNotExists);
		super.dropDatabase(dbName, ignoreIfNotExists);
	}

	@Override
	public boolean dbExists(String dbName) {
		return inMemoryCatalog.dbExists(dbName) && super.dbExists(dbName);
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

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException {
		if (!dbExists(functionPath.getDbName())) {
			throw new DatabaseNotExistException(catalogName, functionPath.getDbName());
		} else {
			if (function instanceof FlinkTempFunction) {
				inMemoryCatalog.createFunction(functionPath, function, ignoreIfExists);
			} else {
				try {
					client.createFunction(createHiveFunction(functionPath, function));
				} catch (AlreadyExistsException e) {
					if (!ignoreIfExists) {
						throw new FunctionAlreadyExistException(catalogName, functionPath.getFullName());
					}
				} catch (TException e) {
					throw new FlinkHiveException(String.format("Failed to create function %s", functionPath.getFullName()), e);
				}
			}
		}
	}

	@Override
	public void alterFunction(ObjectPath path, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException {
		// Needs explicit check since alterFunction() does not throw UnknownDBException when the database does not exist
		// Check database and function together to simplify logic
		if (functionExists(path)) {
			if (inMemoryCatalog.functionExists(path)) {
				if (newFunction instanceof FlinkTempFunction) {
					inMemoryCatalog.alterFunction(path, newFunction, ignoreIfNotExists);
				} else {
					// inMemoryCatalog can only store FlinkTempFunction
					throw new IllegalArgumentException(
						String.format("Function %s is a FlinkTempFunction, newFunction is a CatalogFunction", path.getFullName()));
				}
			} else {
				if (!(newFunction instanceof FlinkTempFunction)) {
					try {
						client.alterFunction(path.getDbName(), path.getObjectName(), createHiveFunction(path, newFunction));
					} catch (TException e) {
						throw new FlinkHiveException(String.format("Failed to alter function %s", path.getFullName()), e);
					}
				} else {
					// HMS can only store non FlinkTempFunction
					throw new IllegalArgumentException(
						String.format("Function %s is a CatalogFunction, newFunction is a FlinkTempFunction", path.getFullName()));
				}
			}
		} else if (!ignoreIfNotExists) {
			throw new FunctionNotExistException(catalogName, path.getFullName());
		}
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException {
		if (inMemoryCatalog.functionExists(functionPath)) {
			inMemoryCatalog.dropFunction(functionPath, ignoreIfNotExists);
		} else {
			try {
				client.dropFunction(functionPath.getDbName(), functionPath.getObjectName());
			} catch (NoSuchObjectException e) {
				if (!ignoreIfNotExists) {
					throw new FunctionNotExistException(catalogName, functionPath.getFullName());
				}
			} catch (TException e) {
				throw new FlinkHiveException(String.format("Failed to drop function %s", functionPath.getFullName()), e);
			}
		}
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
		if (inMemoryCatalog.functionExists(functionPath)) {
			return inMemoryCatalog.getFunction(functionPath);
		} else {
			return super.getFunction(functionPath);
		}
	}

	@Override
	public List<ObjectPath> listFunctions(String dbName) throws DatabaseNotExistException {
		List<ObjectPath> functions = new ArrayList<>();
		functions.addAll(inMemoryCatalog.listFunctions(dbName));
		functions.addAll(super.listFunctions(dbName));

		return functions;
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) {
		return inMemoryCatalog.functionExists(functionPath) || super.functionExists(functionPath);
	}

	private static Function createHiveFunction(ObjectPath functionPath, CatalogFunction function) {
		// TODO: extract more properties from CatalogFunction and add to Hive Function
		return new Function(
			functionPath.getObjectName(),
			functionPath.getDbName(),
			function.getClazzName(),
			null,
			PrincipalType.GROUP, // Temporarily set to GROUP type because it's required by Hive. May change later
			0,
			FunctionType.JAVA,
			new ArrayList<>());
	}
}
