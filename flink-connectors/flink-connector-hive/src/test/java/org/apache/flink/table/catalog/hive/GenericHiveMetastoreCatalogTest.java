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

import org.apache.flink.table.catalog.CatalogTestBase;

import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Test for GenericHiveMetastoreCatalog.
 *
 * <p>Since GenericHiveMetastoreCatalog is unfinished, we temporarily copy necessary tests from CatalogTestBase.
 * Once GenericHiveMetastoreCatalog is finished, we should remove all unit tests in this class and make this test class
 * extend CatalogTestBase.
 */
public class GenericHiveMetastoreCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createGenericHiveMetastoreCatalog();
		catalog.open();
	}

	public String getTableType() {
		return "generic_hive_metastore";
	}

	// =====================
	// GenericHiveMetastoreCatalog doesn't support stats, partition, view yet
	// Thus, overriding the following tests in CatalogTestBase so they won't run against GenericHiveMetastoreCatalog
	// =====================

	// ------ table and column stats ------

	@Override
	public void testGetTableStats_TableNotExistException() { }

	@Override
	public void testAlterTableStats() { }

	@Override
	public void testAlterTableStats_partitionedTable() { }

	@Override
	public void testAlterTableStats_TableNotExistException() { }

	@Override
	public void testAlterTableStats_TableNotExistException_ignore() { }

	// ------ views ------

	@Override
	public void testCreateView() { }

	@Override
	public void testCreateView_DatabaseNotExistException() { }

	@Override
	public void testCreateView_TableAlreadyExistException() { }

	@Override
	public void testCreateView_TableAlreadyExist_ignored() { }

	@Override
	public void testDropView() { }

	@Override
	public void testAlterView() { }

	@Override
	public void testAlterView_TableNotExistException() { }

	@Override
	public void testAlterView_TableNotExist_ignored() { }

	@Override
	public void testListView() { }

	// ------ partitions ------

	@Override
	public void testCreatePartition() { }

	@Override
	public void testCreateParition_TableNotExistException() { }

	@Override
	public void testCreateParition_TableNotPartitionedException() { }

	@Override
	public void testCreateParition_PartitionAlreadExistException() { }

	@Override
	public void testCreateParition_PartitionAlreadExist_ignored() { }

	@Override
	public void testDropPartition() { }

	@Override
	public void testDropPartition_TableNotExistException() { }

	@Override
	public void testDropPartition_TableNotPartitionedException() { }

	@Override
	public void testDropPartition_PartitionNotExistException() { }

	@Override
	public void testDropPartition_PartitionNotExist_ignored() { }

	@Override
	public void testAlterPartition() { }

	@Override
	public void testAlterPartition_TableNotExistException() { }

	@Override
	public void testAlterPartition_TableNotPartitionedException() { }

	@Override
	public void testAlterPartition_PartitionNotExistException() { }

	@Override
	public void testAlterPartition_PartitionNotExist_ignored() { }

	@Override
	public void testGetPartition_TableNotExistException() { }

	@Override
	public void testGetPartition_TableNotPartitionedException() { }

	@Override
	public void testGetParition_PartitionNotExistException() { }

	@Override
	public void testPartitionExists() { }
}
