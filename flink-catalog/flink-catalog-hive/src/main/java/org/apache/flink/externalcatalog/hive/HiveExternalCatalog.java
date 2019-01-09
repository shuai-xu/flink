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

package org.apache.flink.externalcatalog.hive;

import org.apache.flink.table.api.CatalogAlreadyExistException;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.InvalidFunctionException;
import org.apache.flink.table.api.PartitionAlreadyExistException;
import org.apache.flink.table.api.PartitionNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CrudExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogFunction;
import org.apache.flink.table.catalog.ExternalCatalogTablePartition;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

/**
 * Hive External Catalog. A catalog represents a hive database.
 * This class is a READ ONLY database which do not support drop table.
 */
public class HiveExternalCatalog implements CrudExternalCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(HiveExternalCatalog.class);
	public static final String DEFAULT = "default";
	private final HiveConf hiveConf;
	private final String database;
	private IMetaStoreClient metaStoreClient;

	/**
	 * Hive External Catalog, the constructor.
	 *
	 * @param database The name of the database
	 * @param hiveConf The hive configuration
	 */
	public HiveExternalCatalog(
			String database,
			HiveConf hiveConf) {

		if (StringUtils.isEmpty(database)) {
			database = DEFAULT;
		}
		if (null == hiveConf) {
			hiveConf = new HiveConf();
		}
		this.database = database;
		this.hiveConf = hiveConf;
	}

	/**
	 * Get MetaStore Client.
	 *
	 * @return HiveMetaStoreClient instance
	 */
	protected IMetaStoreClient getMSC() {

		if (null == metaStoreClient) {
			try {
				metaStoreClient = RetryingMetaStoreClient.getProxy(
						hiveConf,
						null,
						null,
						HiveMetaStoreClient.class.getName(),
						true);
			} catch (MetaException e) {
				throw new RuntimeException(e);
			}
		}
		return metaStoreClient;
	}

	@Override
	public void createFunction(
			String functionName,
			String className,
			boolean ignoreIfExists)
		throws FunctionAlreadyExistException, InvalidFunctionException {

		LOG.info("create function, function name={}, class name={}", functionName, className);
		Function func = new Function(
			functionName,
			database,
			className,
			"Flink", // TODO pass the user name
			PrincipalType.USER,
			(int) (System.currentTimeMillis() / 1000),
			org.apache.hadoop.hive.metastore.api.FunctionType.JAVA,
			null  // TODO pass the resources
		);

		try {
			getMSC().createFunction(func);
		} catch (AlreadyExistsException e) {
			if (ignoreIfExists) {
				return;
			} else {
				throw new FunctionAlreadyExistException(database, functionName, e);
			}
		} catch (TException e) {
			throw new InvalidFunctionException(database, functionName, className, e);
		}
	}

	@Override
	public void dropFunction(
			String functionName,
			boolean ignoreIfNotExists) throws FunctionNotExistException {

		LOG.info("dropFunction, functionName={}, ignoreIfNotExists={}",
			functionName, ignoreIfNotExists);

		throw new UnsupportedOperationException("Drop Function is not supported!");
	}

	@Override
	public ExternalCatalogFunction getFunction(String functionName) {

		try {
			Function function = getMSC().getFunction(database, functionName);
			return this.toExternalCatalogFunction(function);
		} catch (TException e) {
			throw new FunctionNotExistException(database, functionName, e);
		}
	}

	@Override
	public List<ExternalCatalogFunction> listFunctions() {
		List<ExternalCatalogFunction> externalCatalogFunctions = new ArrayList<>();
		try {
			List<Function> functions = getMSC().getAllFunctions().getFunctions();
			if (functions != null && !functions.isEmpty()) {
				for (Function function : functions) {
					externalCatalogFunctions.add(this.toExternalCatalogFunction(function));
				}
			}
		} catch (TException e) {
			throw new InvalidFunctionException(database, "", "", e);
		}
		return externalCatalogFunctions;
	}

	@Override
	public void createPartition(
			String tableName,
			ExternalCatalogTablePartition part,
			boolean ignoreIfExists)
			throws CatalogNotExistException, TableNotExistException, PartitionAlreadyExistException {

		LOG.info("createPartition, table={}, partition={}, ignoreIfExists={}",
				tableName, part.partitionSpec().toString(), ignoreIfExists);

		Partition partition = new Partition();
		partition.setDbName(database);
		partition.setTableName(tableName);
		try {
			Table table = getMSC().getTable(database, tableName);
			List<FieldSchema> partKeys = table.getPartitionKeys();
			LinkedHashMap<String, String> partSpec = part.partitionSpec();
			List<String> values = new ArrayList<>();
			for (FieldSchema partKey : partKeys) {
				values.add(partSpec.get(partKey.getName()));
			}
			partition.setValues(values);
			partition.setSd(table.getSd());
			partition.getSd().setLocation(null);
			partition.setParameters(part.properties());
		} catch (NoSuchObjectException e) {
			if (ignoreIfExists) {
				return;
			} else {
				throw new TableNotExistException(database, tableName, e);
			}
		} catch (TException e) {
			throw new CatalogNotExistException(database, e);
		}

		try {
			getMSC().add_partition(partition);
		} catch (InvalidObjectException e) {
			throw new TableNotExistException(database, tableName, e);
		} catch (AlreadyExistsException e) {
			if (ignoreIfExists) {
				return;
			} else {
				throw new PartitionAlreadyExistException(database, tableName, part.partitionSpec(), e);
			}
		} catch (TException e) {
			throw new CatalogNotExistException(database, e);
		}
	}

	@Override
	public void dropPartition(
			String tableName,
			LinkedHashMap<String, String> partSpec,
			boolean ignoreIfNotExists)
			throws CatalogNotExistException, TableNotExistException, PartitionNotExistException {

		LOG.info("dropPartition, tableName={}, partSpec={}, ignoreIfNotExists={}",
				tableName, partSpec.toString(), ignoreIfNotExists);

		List<String> values = new ArrayList<>();
		for (Map.Entry<String, String> kv : partSpec.entrySet()) {
			values.add(kv.getValue());
		}

		try {
			getMSC().dropPartition(database, tableName, values, true);
		} catch (NoSuchObjectException e) {
			if (ignoreIfNotExists) {
				return;
			} else {
				throw new PartitionNotExistException(database, tableName, partSpec, e);
			}
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void alterPartition(
			String tableName,
			ExternalCatalogTablePartition part,
			boolean ignoreIfNotExists)
			throws CatalogNotExistException, TableNotExistException, PartitionNotExistException {

		LOG.info("alterPartition, tableName={}, partition={}, ignoreIfNotExists={}",
				tableName, part.partitionSpec(), ignoreIfNotExists);

		List<String> values = new ArrayList<>();
		for (Map.Entry<String, String> kv : part.partitionSpec().entrySet()) {
			values.add(kv.getValue());
		}
		Partition partition;
		try {
			partition = getMSC().getPartition(database, tableName, values);
		} catch (TException e) {
			throw new PartitionNotExistException(database, tableName, part.partitionSpec());
		}
		partition.setParameters(part.properties());
		try {
			getMSC().alter_partition(database, tableName, partition);
		} catch (InvalidOperationException e) {
			if (ignoreIfNotExists) {
				return;
			} else {
				throw new PartitionNotExistException(database, tableName, part.partitionSpec());
			}
		} catch (TException e) {
			throw new CatalogNotExistException(database, e);
		}
	}

	@Override
	public void createTable(
			String tableName,
			CatalogTable table,
			boolean ignoreIfExists)
			throws TableAlreadyExistException {

		LOG.info("createTable, tableName={}, ignoreIfExists={}",
				tableName, ignoreIfExists);

		try {
			Table hiveTable = MetaConverter.convertToHiveTable(
					table, database, tableName);
			getMSC().createTable(hiveTable);
		} catch (AlreadyExistsException e) {
			if (ignoreIfExists) {
				return;
			} else {
				throw new TableAlreadyExistException(database, tableName, e);
			}
		} catch (TException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void dropTable(
			String tableName,
			boolean ignoreIfNotExists) throws TableNotExistException {

		LOG.info("dropTable, tableName={}, ignoreIfNotExists={}",
				tableName, ignoreIfNotExists);

		throw new UnsupportedOperationException("Drop Table is not supported!");
	}

	@Override
	public void alterTable(
			String tableName,
			CatalogTable table,
			boolean ignoreIfNotExists) throws TableNotExistException {

		LOG.info("alterTable, tableName={}, table={}, ignoreIfNotExists={}",
				tableName, table.toString(), ignoreIfNotExists);

		throw new UnsupportedOperationException("Alter Table is not supported!");
	}

	@Override
	public void alterTableStats(
			String tableName,
			Option<TableStats> stats,
			boolean ignoreIfNotExists) throws TableNotExistException {
		LOG.info("alterTableStats, tableName={}, stats={}", tableName, stats);

		try {
			// Alter table level statistics
			Table table = getMSC().getTable(database, tableName);
			List<FieldSchema> fields = table.getSd().getCols();

			if (stats == null || stats.isEmpty()) {
				// Delete stats
				table.getParameters().put(
					StatsSetupConst.ROW_COUNT,
					"0");
				getMSC().alter_table(database, tableName, table);

				for (FieldSchema field : fields) {
					String colName = field.getName();  // Hive
					try {
						getMSC().deleteTableColumnStatistics(database, tableName, colName);
					} catch (NoSuchObjectException e) {
					}
				}
				return;
			}

			TableStats tableStats = stats.get();
			table.getParameters().put(
				StatsSetupConst.ROW_COUNT,
				tableStats.rowCount().toString());
			getMSC().alter_table(database, tableName, table);

			Map<String, ColumnStats> colStats = tableStats.colStats();

			if (colStats == null || colStats.isEmpty()) {
				return;
			}

			ColumnStatistics columnStatistics =
				MetaConverter.convertFlinkStatsToHiveStats(
					database, tableName, colStats, fields);
			getMSC().updateTableColumnStatistics(columnStatistics);

		} catch (TException e) {

			LOG.error("Exception happens while updating the statistics:", e);
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(database, tableName, e);
			}
		}
	}

	@Override
	public void createSubCatalog(
			String name,
			ExternalCatalog catalog,
			boolean ignoreIfExists) throws CatalogAlreadyExistException {

		LOG.info("createSubCatalog, name={}, catalog={}, ignoreIfExists={}",
				name, catalog.toString(), ignoreIfExists);

		Database db = new Database(
				name,
				"flink: " + name,
				hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE) + "/" + name,
				new HashMap<>());
		try {
			getMSC().createDatabase(db);
		} catch (AlreadyExistsException e) {
			if (ignoreIfExists) {
				return;
			} else {
				throw new CatalogAlreadyExistException(name, e);
			}
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void dropSubCatalog(
			String name,
			boolean ignoreIfNotExists) throws CatalogNotExistException {

		LOG.info("dropSubCatalog, name={}, ignoreIfNotExists={}",
				name, ignoreIfNotExists);

		try {
			getMSC().dropDatabase(name);
		} catch (NoSuchObjectException e) {
			if (ignoreIfNotExists) {
				return;
			} else {
				throw new CatalogNotExistException(name, e);
			}
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void alterSubCatalog(
			String name,
			ExternalCatalog catalog,
			boolean ignoreIfNotExists) throws CatalogNotExistException {

		LOG.info("alterSubCatalog, name={}, catalog={}, ignoreIfNotExists={}",
				name, catalog.toString(), ignoreIfNotExists);

		Database db = new Database(
				name,
				"flink: " + name,
				hiveConf.getVar(HiveConf.ConfVars.METASTOREWAREHOUSE) + "/" + name,
				new HashMap<>());
		try {
			getMSC().alterDatabase(name, db);
		} catch (NoSuchObjectException e) {
			if (ignoreIfNotExists) {
				return;
			} else {
				throw new CatalogNotExistException(name, e);
			}
		} catch (TException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public ExternalCatalogTablePartition getPartition(
			String tableName,
			LinkedHashMap<String, String> partSpec)
			throws CatalogNotExistException, TableNotExistException, PartitionNotExistException {

		LOG.info("getPartition, name={}, partSpec={}",
			tableName, partSpec);

		List<String> partVals = new ArrayList<>();
		for (Map.Entry<String, String> val : partSpec.entrySet()) {
			partVals.add(val.getValue());
		}

		try {
			Partition part = getMSC().getPartition(database, tableName, partVals);

			return MetaConverter.convertToExternalCatalogPartition(
					part, partSpec, getMSC());
		} catch (NoSuchObjectException e) {
			throw new PartitionNotExistException(database, tableName, partSpec);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<LinkedHashMap<String, String>> listPartitions(String tableName)
			throws CatalogNotExistException, TableNotExistException {

		LOG.info("listPartitions, tableName={}", tableName);

		List<LinkedHashMap<String, String>> list = new ArrayList<>();
		try {
			Table table = getMSC().getTable(database, tableName);
			List<FieldSchema> partKeys = table.getPartitionKeys();
			List<String> partKeyStrs = new ArrayList<>();
			for (FieldSchema partKey : partKeys) {
				partKeyStrs.add(partKey.getName());
			}

			List<Partition> partList = getMSC().listPartitions(database, tableName, Short.MAX_VALUE);
			for (Partition part : partList) {
				int i = 0;
				LinkedHashMap<String, String> partMap = new LinkedHashMap<>();
				for (String s : part.getValues()) {
					partMap.put(partKeyStrs.get(i), s);
					i++;
				}
				list.add(partMap);
			}
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(database, tableName, e);
		} catch (TException e) {
			throw new RuntimeException(e);
		}

		return list;
	}

	@Override
	public CatalogTable getTable(String tableName) throws TableNotExistException {

		LOG.info("getTable, tableName={}", tableName);

		try {
			Table table = getMSC().getTable(database, tableName);
			return MetaConverter.convertToExternalCatalogTable(table, getMSC());
		} catch (NoSuchObjectException e) {
			throw new TableNotExistException(database, tableName, e);
		} catch (TException | IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> listTables() {

		LOG.info("listTables");

		try {
			return getMSC().getAllTables(database);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {

		LOG.info("getSubCatalog, dbName={}", dbName);

		try {
			getMSC().getDatabase(dbName);
		} catch (NoSuchObjectException e) {
			throw new CatalogNotExistException(dbName, e);
		} catch (TException e) {
			throw new RuntimeException(e);
		}

		return new HiveExternalCatalog(dbName, hiveConf);
	}

	@Override
	public List<String> listSubCatalogs() {

		LOG.info("listSubCatalogs");

		try {
			return getMSC().getAllDatabases();
		} catch (TException e) {
			throw new RuntimeException(e);
		}
	}

	private ExternalCatalogFunction toExternalCatalogFunction(Function function){
		return new ExternalCatalogFunction(
			function.getDbName(),
			function.getFunctionName(),
			function.getClassName(),
			function.getOwnerName(),
			function.getCreateTime() * 1000L);
	}
}
