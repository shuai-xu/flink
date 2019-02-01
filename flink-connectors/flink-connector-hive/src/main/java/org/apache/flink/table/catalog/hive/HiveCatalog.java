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
import org.apache.flink.table.api.FunctionAlreadyExistException;
import org.apache.flink.table.api.FunctionNotExistException;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.exceptions.PartitionAlreadyExistException;
import org.apache.flink.table.api.exceptions.PartitionNotExistException;
import org.apache.flink.table.api.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A catalog that connects to Hive MetaStore and reads/writes Hive tables.
 */
public class HiveCatalog extends HiveCatalogBase {
	private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

	public HiveCatalog(String catalogName, String hiveMetastoreURI) {
		super(catalogName, hiveMetastoreURI);
		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	public HiveCatalog(String catalogName, HiveConf hiveConf) {
		super(catalogName, hiveConf);
		LOG.info("Created HiveCatalog '{}'", catalogName);
	}

	// ------ tables and views ------

	@Override
	public CatalogTable getTable(ObjectPath path) throws TableNotExistException {
		Table hiveTable = getHiveTable(path);

		TableSchema tableSchema = HiveCatalogUtil.createTableSchema(hiveTable.getSd().getCols(), hiveTable.getPartitionKeys());

		TableStats tableStats = null;
		if (!isTablePartitioned(hiveTable)) {
			// Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
			Map<String, ColumnStats> colStats = HiveCatalogUtil.createColumnStats(
					getHiveTableColumnStats(path.getDbName(), path.getObjectName(), Arrays.asList(tableSchema.getFieldNames())));
			tableStats = TableStats.builder().rowCount(getRowCount(hiveTable)).colStats(colStats).build();
		} else {
			// TableStats of partitioned table is unknown, the behavior is same as HIVE
			tableStats = TableStats.UNKNOWN();
		}

		CatalogTable catalogTable = HiveCatalogUtil.createCatalogTable(hiveTable, tableSchema, tableStats);
		catalogTable.getProperties().put(HiveConf.ConfVars.METASTOREURIS.varname,
			hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
		return catalogTable;
	}

	/**
	 * Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
	 */
	private List<ColumnStatisticsObj> getHiveTableColumnStats(String dbName, String tableName, List<String> cols) {
		try {
			return client.getTableColumnStatistics(dbName, tableName, cols);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get table column stats for columns %s of table %s", cols,
					new ObjectPath(dbName, tableName)));
		}
	}

	// ------ tables ------

	@Override
	public void createTable(ObjectPath path, CatalogTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException {

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

				client.createTable(HiveCatalogUtil.createHiveTable(path, table));
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to create table %s", path.getFullName()), e);
		}
	}

	@Override
	public void alterTable(ObjectPath path, CatalogTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			// alter_table() doesn't throw a clear exception when target table doesn't exist. Thus, check the table existence explicitly
			if (tableExists(path)) {
				client.alter_table(path.getDbName(), path.getObjectName(), HiveCatalogUtil.createHiveTable(path, newTable));
			} else if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to alter table %s", path.getFullName()), e);
		}
	}

	// ------ views ------

	@Override
	public void createView(ObjectPath viewPath, CatalogView view, boolean ignoreIfExists) {
		createTable(viewPath, view, ignoreIfExists);
	}

	@Override
	public void alterView(ObjectPath viewPath, CatalogView newView, boolean ignoreIfNotExists) {
		alterTable(viewPath, newView, ignoreIfNotExists);
	}

	// ------ table and column stats ------

	@Override
	public TableStats getTableStats(ObjectPath path) throws TableNotExistException {
		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			// Get the column statistics for a set of columns in a table. This only works for non-partitioned tables.
			// For partitioned tables, get partition columns stats from HiveCatalog.getPartition()
			Map<String, ColumnStats> colStats = new HashMap<>();
			colStats = HiveCatalogUtil.createColumnStats(
				getHiveTableColumnStats(
					path.getDbName(),
					path.getObjectName(),
					hiveTable.getSd().getCols().stream()
						.map(fs -> fs.getName())
						.collect(Collectors.toList())
				));
			return TableStats.builder().rowCount(getRowCount(hiveTable)).colStats(colStats).build();
		} else {
			// TableStats of partitioned table is unknown, the behavior is same as HIVE
			return TableStats.UNKNOWN();
		}
	}

	@Override
	public void alterTableStats(ObjectPath path, TableStats newtTableStats, boolean ignoreIfNotExists) throws TableNotExistException {
		try {
			Table hiveTable = getHiveTable(path);

			// Set table column stats. This only works for non-partitioned tables.
			if (!isTablePartitioned(hiveTable)) {
				client.updateTableColumnStatistics(HiveCatalogUtil.createColumnStats(hiveTable, newtTableStats.colStats()));
			}

			// Set table stats
			String oldRowCount = hiveTable.getParameters().getOrDefault(StatsSetupConst.ROW_COUNT, "0");
			if (newtTableStats.rowCount() != Long.parseLong(oldRowCount)) {
				hiveTable.getParameters().put(StatsSetupConst.ROW_COUNT, String.valueOf(newtTableStats.rowCount()));
				client.alter_table(path.getDbName(), path.getObjectName(), hiveTable);
			}
		} catch (TableNotExistException e) {
			if (!ignoreIfNotExists) {
				throw new TableNotExistException(catalogName, path.getFullName());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to alter table stats of table %s", path.getFullName()), e);
		}
	}

	// ------ partitions ------

	@Override
	public void createPartition(ObjectPath path, CatalogPartition partition, boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionAlreadyExistException {

		Table hiveTable = getHiveTable(path);

		if (hiveTable.getPartitionKeysSize() == 0) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			client.add_partition(
				HiveCatalogUtil.createHivePartition(hiveTable, partition));
		} catch (AlreadyExistsException e) {
			if (!ignoreIfExists) {
				throw new PartitionAlreadyExistException(catalogName, path, partition.getPartitionSpec());
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to create partition %s of table %s", partition.getPartitionSpec(), path));
		}
	}

	@Override
	public void dropPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			client.dropPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec), true);
		} catch (NoSuchObjectException e) {
			if (!ignoreIfNotExists) {
				throw new PartitionNotExistException(catalogName, path, partitionSpec);
			}
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to drop partition %s of table %s", partitionSpec, path));
		}
	}

	@Override
	public void alterPartition(ObjectPath path, CatalogPartition newPartition, boolean ignoreIfNotExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {
		Table hiveTable = getHiveTable(path);

		if (hiveTable.getPartitionKeysSize() == 0) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		// Explicitly check if the parititon exists or not
		// because alter_partition() doesn't throw NoSuchObjectException like dropPartition() when the target doesn't exist
		if (partitionExists(path, newPartition.getPartitionSpec())) {
			try {
				client.alter_partition(
					path.getDbName(),
					path.getObjectName(),
					HiveCatalogUtil.createHivePartition(hiveTable, newPartition)
				);
			} catch (TException e) {
				throw new FlinkHiveException(
					String.format("Failed to alter existing partition with new partition %s of table %s", newPartition.getPartitionSpec(), path), e);
			}
		} else if (!ignoreIfNotExists) {
			throw new PartitionNotExistException(catalogName, path, newPartition.getPartitionSpec());
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path)
		throws TableNotExistException, TableNotPartitionedException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), (short) -1).stream()
				.map(n -> HiveCatalogUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list partitions of table %s", path), e);
		}
	}

	@Override
	public List<CatalogPartition.PartitionSpec> listPartitions(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return client.listPartitionNames(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec), (short) -1).stream()
				.map(n -> HiveCatalogUtil.createPartitionSpec(n))
				.collect(Collectors.toList());
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to list partitions of table %s", path), e);
		}
	}

	@Override
	public CatalogPartition getPartition(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, PartitionNotExistException {

		Table hiveTable = getHiveTable(path);

		if (!isTablePartitioned(hiveTable)) {
			throw new TableNotPartitionedException(catalogName, path);
		}

		try {
			return HiveCatalogUtil.createCatalogPartition(
				partitionSpec,
				client.getPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec)));
		} catch (NoSuchObjectException e) {
			throw new PartitionNotExistException(catalogName, path, partitionSpec);
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get partition %s of table %s", partitionSpec, path), e);
		}
	}

	@Override
	public boolean partitionExists(ObjectPath path, CatalogPartition.PartitionSpec partitionSpec) {
		try {
			Table hiveTable = getHiveTable(path);
			return client.getPartition(path.getDbName(), path.getObjectName(), getOrderedPartitionValues(hiveTable, partitionSpec)) != null;
		} catch (NoSuchObjectException | TableNotExistException e) {
			return false;
		} catch (TException e) {
			throw new FlinkHiveException(
				String.format("Failed to get partition %s of table %s", partitionSpec, path), e);
		}
	}

	private boolean isTablePartitioned(Table hiveTable) throws TableNotExistException {
		return hiveTable.getPartitionKeysSize() != 0;
	}

	private List<String> getOrderedPartitionValues(Table hiveTable, CatalogPartition.PartitionSpec partitionSpec) {
		return partitionSpec.getOrderedValues(HiveCatalogUtil.getPartitionKeys(hiveTable.getPartitionKeys()));
	}

	// ------ functions ------

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) {
		throw new UnsupportedOperationException();
	}
}
