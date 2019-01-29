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

	public void testGetTableStats_TableNotExistException() { }

	public void testAlterTableStats() { }

	public void testAlterTableStats_partitionedTable() { }

	public void testAlterTableStats_TableNotExistException() { }

	public void testAlterTableStats_TableNotExistExceptio_2() { }

	public void testAlterTableStats_TableNotExistExceptio_ignore() { }

	// ------ views ------

	public void testCreateView() { }

	public void testCreateView_DatabaseNotExistException() { }

	public void testCreateView_TableAlreadyExistException() { }

	public void testCreateView_TableAlreadyExist_ignored() { }

	public void testDropView() { }

	public void testAlterView() { }

	public void testAlterView_TableNotExistException() { }

	public void testAlterView_TableNotExist_ignored() { }

	public void testListView() { }

	// ------ partitions ------

	public void testCreatePartition() { }

	public void testCreateParition_TableNotExistException() { }

	public void testCreateParition_TableNotPartitionedException() { }

	public void testCreateParition_PartitionAlreadExistException() { }

	public void testCreateParition_PartitionAlreadExist_ignored() { }

	public void testDropPartition() { }

	public void testDropPartition_TableNotExistException() { }

	public void testDropPartition_TableNotPartitionedException() { }

	public void testDropPartition_PartitionNotExistException() { }

	public void testDropPartition_PartitionNotExist_ignored() { }

	public void testAlterPartition() { }

	public void testAlterPartition_TableNotExistException() { }

	public void testAlterPartition_TableNotPartitionedException() { }

	public void testAlterPartition_PartitionNotExistException() { }

	public void testAlterPartition_PartitionNotExist_ignored() { }

	public void testGetPartition_TableNotExistException() { }

	public void testGetPartition_TableNotPartitionedException() { }

	public void testGetParition_PartitionNotExistException() { }

	public void testPartitionExists() { }
}
