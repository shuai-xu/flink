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

package org.apache.flink.table.catalog


import java.util.{LinkedHashMap => JLinkedHashMap, LinkedHashSet => JLinkedHashSet}

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import org.apache.flink.table.api._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.plan.stats.TableStats
import org.junit.Assert._
import org.junit.{Before, Test}

class InMemoryExternalCatalogTest {

  private val databaseName = "db1"

  private var catalog: InMemoryExternalCatalog = _

  @Before
  def setUp(): Unit = {
    catalog = new InMemoryExternalCatalog(databaseName)
  }

  @Test
  def testCreatePartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).isEmpty)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("hour", "12", "ds", "2016-02-01"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testCreatePartitionOnUnPartitionedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createNonPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCreateInvalidPartitionSpec(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "h", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
  }

  @Test(expected = classOf[PartitionAlreadyExistException])
  def testCreateExistedPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val newPartitionSpec1 = new JLinkedHashMap[String, String](
      ImmutableMap.of("hour", "12", "ds", "2016-02-01"))
    val newPartition1 = ExternalCatalogTablePartition(newPartitionSpec1)
    catalog.createPartition(tableName, newPartition1, false)
  }

  @Test(expected = classOf[TableNotExistException])
  def testCreatePartitionOnNotExistTable(): Unit = {
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition("notexistedTb", newPartition, false)
  }

  @Test
  def testGetPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
    assertEquals(catalog.getPartition(tableName, newPartitionSpec), newPartition)
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testGetNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    assertEquals(catalog.getPartition(tableName, newPartitionSpec), newPartition)
  }

  @Test
  def testDropPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).contains(newPartitionSpec))
    catalog.dropPartition(tableName, newPartitionSpec, ignoreIfNotExists = false)
    assertFalse(catalog.listPartitions(tableName).contains(newPartitionSpec))
  }

  @Test(expected = classOf[PartitionNotExistException])
  def testDropNotExistPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val partitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    catalog.dropPartition(tableName, partitionSpec, ignoreIfNotExists = false)
  }

  @Test
  def testListPartitionSpec(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    assertTrue(catalog.listPartitions(tableName).isEmpty)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(newPartitionSpec)
    catalog.createPartition(tableName, newPartition, false)
    val partitionSpecs = catalog.listPartitions(tableName)
    assertEquals(partitionSpecs.size(), 1)
    assertEquals(partitionSpecs.get(0), newPartitionSpec)
  }

  @Test
  def testAlterPartition(): Unit = {
    val tableName = "t1"
    catalog.createTable(
      tableName,
      createPartitionedTableInstance,
      ignoreIfExists = false)
    val newPartitionSpec = new JLinkedHashMap[String, String](
      ImmutableMap.of("ds", "2016-02-01", "hour", "12"))
    val newPartition = ExternalCatalogTablePartition(
      newPartitionSpec,
      properties = ImmutableMap.of("location", "/tmp/ds=2016-02-01/hour=12"))
    catalog.createPartition(tableName, newPartition, false)
    val updatedPartition = ExternalCatalogTablePartition(
      newPartitionSpec,
      properties = ImmutableMap.of("location", "/tmp1/ds=2016-02-01/hour=12"))
    catalog.alterPartition(tableName, updatedPartition, false)
    val currentPartition = catalog.getPartition(tableName, newPartitionSpec)
    assertEquals(currentPartition, updatedPartition)
    assertNotEquals(currentPartition, newPartition)
  }

  @Test
  def testCreateTable(): Unit = {
    assertTrue(catalog.listTables().isEmpty)
    catalog.createTable("t1", createTableInstance(), ignoreIfExists = false)
    val tables = catalog.listTables()
    assertEquals(1, tables.size())
    assertEquals("t1", tables.get(0))
  }

  @Test(expected = classOf[TableAlreadyExistException])
  def testCreateExistedTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
  }

  @Test
  def testGetTable(): Unit = {
    val originTable = createTableInstance()
    catalog.createTable("t1", originTable, ignoreIfExists = false)
    assertEquals(catalog.getTable("t1"), originTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testGetNotExistTable(): Unit = {
    catalog.getTable("nonexisted")
  }

  @Test
  def testAlterTable(): Unit = {
    val tableName = "t1"
    val table = createTableInstance()
    catalog.createTable(tableName, table, ignoreIfExists = false)
    assertEquals(catalog.getTable(tableName), table)
    val newTable = createOtherTableInstance()
    catalog.alterTable(tableName, newTable, ignoreIfNotExists = false)
    val currentTable = catalog.getTable(tableName)
    // validate the table is really replaced after alter table
    assertNotEquals(table, currentTable)
    assertEquals(newTable, currentTable)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterNotExistTable(): Unit = {
    catalog.alterTable("nonexisted", createTableInstance(), ignoreIfNotExists = false)
  }

  @Test
  def testDropTable(): Unit = {
    val tableName = "t1"
    catalog.createTable(tableName, createTableInstance(), ignoreIfExists = false)
    assertTrue(catalog.listTables().contains(tableName))
    catalog.dropTable(tableName, ignoreIfNotExists = false)
    assertFalse(catalog.listTables().contains(tableName))
  }

  @Test(expected = classOf[TableNotExistException])
  def testDropNotExistTable(): Unit = {
    catalog.dropTable("nonexisted", ignoreIfNotExists = false)
  }

  @Test(expected = classOf[CatalogNotExistException])
  def testGetNotExistDatabase(): Unit = {
    catalog.getSubCatalog("notexistedDb")
  }

  @Test
  def testCreateDatabase(): Unit = {
    catalog.createSubCatalog("db2", new InMemoryExternalCatalog("db2"), ignoreIfExists = false)
    assertEquals(1, catalog.listSubCatalogs().size)
  }

  @Test(expected = classOf[CatalogAlreadyExistException])
  def testCreateExistedDatabase(): Unit = {
    catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"),
      ignoreIfExists = false)

    assertNotNull(catalog.getSubCatalog("existed"))
    val databases = catalog.listSubCatalogs()
    assertEquals(1, databases.size())
    assertEquals("existed", databases.get(0))

    catalog.createSubCatalog("existed", new InMemoryExternalCatalog("existed"),
      ignoreIfExists = false)
  }

  @Test
  def testNestedCatalog(): Unit = {
    val sub = new InMemoryExternalCatalog("sub")
    val sub1 = new InMemoryExternalCatalog("sub1")
    catalog.createSubCatalog("sub", sub, ignoreIfExists = false)
    sub.createSubCatalog("sub1", sub1, ignoreIfExists = false)
    sub1.createTable("table", createTableInstance(), ignoreIfExists = false)
    val tables = catalog.getSubCatalog("sub").getSubCatalog("sub1").listTables()
    assertEquals(1, tables.size())
    assertEquals("table", tables.get(0))
  }

  @Test
  def testAlterTableStats(): Unit = {
    val tableName = "t1"
    val table = createTableInstance()
    assertNull(table.stats)
    catalog.createTable(tableName, table, ignoreIfExists = false)
    assertEquals(catalog.getTable(tableName), table)

    val newTableStats = new TableStats(1000L)
    catalog.alterTableStats(tableName, Some(newTableStats), ignoreIfNotExists = false)
    val currentTable = catalog.getTable(tableName)
    assertNotEquals(table, currentTable)
    assertEquals(table.copy(stats = newTableStats), currentTable)
    assertEquals(newTableStats, currentTable.stats)

    // update TableStats with None
    catalog.alterTableStats(tableName, None, ignoreIfNotExists = false)
    val currentTable2 = catalog.getTable(tableName)
    assertEquals(table, currentTable2)
    assertNull(currentTable2.stats)
  }

  @Test(expected = classOf[TableNotExistException])
  def testAlterTableStatsWithNotExistTable(): Unit = {
    catalog.alterTableStats("nonexisted", Some(new TableStats(1000L)), ignoreIfNotExists = false)
  }

  @Test
  def testCreateFunction(): Unit = {
    assertTrue(catalog.listFunctions().isEmpty)
    val functionName = "concatString"
    val className = "org.apache.flink.table.catalog.test.functions.StringFunction"
    catalog.createFunction(functionName, className, false)

    val functions = catalog.listFunctions()
    assertEquals(1, functions.size())
    assertEquals(functionName, functions.get(0).funcName)
    assertEquals(className, functions.get(0).className)
    assertEquals(functions.get(0), catalog.getFunction(functionName))
  }

  @Test(expected = classOf[FunctionAlreadyExistException])
  def testCreateExistedFunction(): Unit = {
    val functionName = "concatString"
    val className = "org.apache.flink.table.catalog.test.functions.StringFunction"
    catalog.createFunction(functionName, className, false)
    catalog.createFunction(functionName, className, false)
  }

  @Test
  def testCreateExistedFunctionWithIgnoreIfExists(): Unit = {
    val functionName = "concatString"
    val className = "org.apache.flink.table.catalog.test.functions.StringFunction"
    catalog.createFunction(functionName, className, false)
    catalog.createFunction(functionName, className, true)
  }

  @Test
  def testDropFunction(): Unit = {
    val functionName = "concatString"
    val className = "org.apache.flink.table.catalog.test.functions.StringFunction"

    catalog.createFunction(functionName, className, false)
    assertTrue(catalog.getFunction(functionName).funcName.equals(functionName))
    catalog.dropFunction(functionName, ignoreIfNotExists = false)
    assertTrue(catalog.listFunctions().size() == 0)
  }

  @Test(expected = classOf[FunctionNotExistException])
  def testDropNotExistFunction(): Unit = {
    catalog.dropFunction("non_existed_function", ignoreIfNotExists = false)
  }

  @Test
  def testDropNotExistFunctionWithIgnore(): Unit = {
    catalog.dropFunction("non_existed_function", ignoreIfNotExists = true)
  }

  private def createTableInstance(): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.INT
      )
    )
    ExternalCatalogTable("csv", schema)
  }

  private def createOtherTableInstance(): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.STRING  // different from create table instance.
      )
    )
    ExternalCatalogTable("csv", schema)
  }

  private def createNonPartitionedTableInstance: ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.INT
      ))
    ExternalCatalogTable("csv", schema)
  }

  private def createPartitionedTableInstance: ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.INT
      ))
    ExternalCatalogTable(
      "hive",
      schema,
      partitionColumnNames = new JLinkedHashSet(ImmutableSet.of("ds", "hour")),
      isPartitioned = true
    )
  }

}

object InMemoryExternalCatalogTest {
  def createTable(): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.INT
      )
    )
    ExternalCatalogTable("csv", schema)
  }

  def createAnotherTable(): ExternalCatalogTable = {
    val schema = new TableSchema(
      Array("first", "second"),
      Array(
        DataTypes.STRING,
        DataTypes.STRING  // different from create table instance.
      )
    )
    ExternalCatalogTable("csv", schema)
  }
}
