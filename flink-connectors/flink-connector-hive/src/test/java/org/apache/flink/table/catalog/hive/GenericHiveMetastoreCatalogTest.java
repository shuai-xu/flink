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
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.FlinkTempFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.apache.flink.table.catalog.CatalogTestUtil.compare;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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

	// ------ functions ------

	@Test
	public void testCreateFunction_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.functionExists(path1));

		catalog.createFunction(path1, createTempFunction(), false);

		assertTrue(catalog.functionExists(path1));
	}

	@Test
	public void testCreateFunction_DatabaseNotExistException_FlinkTempFunction() {
		assertFalse(catalog.dbExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createFunction(path1, createTempFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExistException_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createTempFunction(), false);

		exception.expect(FunctionAlreadyExistException.class);
		catalog.createFunction(path1, createTempFunction(), false);
	}

	@Test
	public void testCreateFunction_FunctionAlreadyExist_ignored_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);

		FlinkTempFunction func = createTempFunction();
		catalog.createFunction(path1, func, false);

		compare(func, (FlinkTempFunction) catalog.getFunction(path1));

		catalog.createFunction(path1, createAnotherTempFunction(), true);

		compare(func, (FlinkTempFunction) catalog.getFunction(path1));
	}

	@Test
	public void testAlterFunction_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);

		FlinkTempFunction func = createTempFunction();
		catalog.createFunction(path1, func, false);

		compare(func, (FlinkTempFunction) catalog.getFunction(path1));

		FlinkTempFunction newFunc = createAnotherTempFunction();
		catalog.alterFunction(path1, newFunc, false);

		assertNotEquals(func, catalog.getFunction(path1));
		compare(newFunc, (FlinkTempFunction) catalog.getFunction(path1));
	}

	@Test
	public void testAlterFunction_FunctionNotExistException_FlinkTempFunction() {
		exception.expect(FunctionNotExistException.class);
		catalog.alterFunction(nonExistObjectPath, createTempFunction(), false);
	}

	@Test
	public void testAlterFunction_FunctionNotExist_ignored_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterFunction(nonExistObjectPath, createTempFunction(), true);

		assertFalse(catalog.functionExists(nonExistObjectPath));
	}

	@Test
	public void testListFunctions_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogFunction func1 = createFunction();
		CatalogFunction func2 = createTempFunction();
		catalog.createFunction(path1, func1, false);
		catalog.createFunction(path3, func2, false);

		assertEquals(new HashSet<>(Arrays.asList(path1, path3)), new HashSet<>(catalog.listFunctions(db1)));
	}

	@Test
	public void testDropFunction_FlinkTempFunction() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createFunction(path1, createTempFunction(), false);

		assertTrue(catalog.functionExists(path1));

		catalog.dropFunction(path1, false);

		assertFalse(catalog.functionExists(path1));
	}

	protected FlinkTempFunction createTempFunction() {
		return new FlinkTempFunction(new MyScalarFunction());
	}

	protected FlinkTempFunction createAnotherTempFunction() {
		return new FlinkTempFunction(new MyOtherScalarFunction());
	}

	/**
	 * Test UDF.
	 */
	public static class MyScalarFunction extends ScalarFunction {
		public Integer eval(Integer i) {
			return i + 1;
		}
	}

	/**
	 * Test UDF.
	 */
	public static class MyOtherScalarFunction extends ScalarFunction {
		public String eval(Integer i) {
			return String.valueOf(i);
		}
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
