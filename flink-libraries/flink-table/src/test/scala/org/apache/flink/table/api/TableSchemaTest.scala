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

package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{Alias, ProctimeAttribute, RowtimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.types.DataTypes
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.Test

class TableSchemaTest extends TableTestBase {

  @Test
  def testStreamTableSchema(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getColumnNames.apply(0))
    assertEquals("b", schema.getColumnNames.apply(1))

    assertEquals(DataTypes.INT, DataTypes.internal(schema.getTypes.apply(0)))
    assertEquals(DataTypes.STRING, DataTypes.internal(schema.getTypes.apply(1)))

    val expectedString = "root\n" +
      " |--    name: a\n"      +
      "    |-- type: IntType\n" +
      "    |-- physicalIndex: 0\n" +
      "    |-- isNullable: true\n" +
      "    |-- isPrimaryKey: false\n" +
      " |--    name: b\n" +
      "    |-- type: StringType\n" +
      "    |-- physicalIndex: 1\n" +
      "    |-- isNullable: true\n" +
      "    |-- isPrimaryKey: false\n"
    assertEquals(expectedString, schema.toString)

    assertEquals("a", schema.getColumnName(0))

    try {
      schema.getColumnNames(-1)
      fail("Should never reach here")
    } catch {
      case _ =>
    }

    try {
      schema.getType(-1)
      fail("Should never reach here")
    } catch {
      case _ =>
    }

    assertTrue(schema.getType("c").isEmpty)
  }


  val tupleType = new TupleTypeInfo(
    INT_TYPE_INFO,
    STRING_TYPE_INFO,
    DOUBLE_TYPE_INFO)

  val rowType = new RowTypeInfo(LONG_TYPE_INFO, STRING_TYPE_INFO,DOUBLE_TYPE_INFO)

  val caseClassType: TypeInformation[CClass] = implicitly[TypeInformation[CClass]]

  val pojoType: TypeInformation[PojoClass] = TypeExtractor.createTypeInfo(classOf[PojoClass])

  val atomicType = INT_TYPE_INFO

  val genericRowType = new GenericTypeInfo[Row](classOf[Row])

  @Test
  def testGetFieldInfoRow(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(rowType)

    tableSchema.getColumnNames.zip(Array("f0", "f1", "f2")).foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoRowNames(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      rowType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
      .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTuple(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(tupleType)

    tableSchema.getColumnNames.zip(Array("f0", "f1", "f2")).foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClass(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(caseClassType)

    tableSchema.getColumnNames.zip(Array("cf1", "cf2", "cf3"))
        .foreach(x => assertEquals(x._2, x._1))

    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojo(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(pojoType)

    tableSchema.getColumnNames.zip(Array("pf1", "pf2", "pf3"))
      .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoAtomic(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(atomicType)

    tableSchema.getColumnNames.zip(Array("f0")).foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleNames(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      tupleType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassNames(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      caseClassType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojoNames2(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      pojoType,
      Array(
        UnresolvedFieldReference("pf3"),
        UnresolvedFieldReference("pf1"),
        UnresolvedFieldReference("pf2")
      ))

    tableSchema.getColumnNames.zip(Array("pf3", "pf1", "pf2"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoAtomicName1(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      atomicType,
      Array(UnresolvedFieldReference("name")))

    tableSchema.getColumnNames.zip(Array("name")).foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleAlias1(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("f0"), "name1"),
        Alias(UnresolvedFieldReference("f1"), "name2"),
        Alias(UnresolvedFieldReference("f2"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTupleAlias2(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("f2"), "name1"),
        Alias(UnresolvedFieldReference("f0"), "name2"),
        Alias(UnresolvedFieldReference("f1"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassAlias1(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("cf1"), "name1"),
        Alias(UnresolvedFieldReference("cf2"), "name2"),
        Alias(UnresolvedFieldReference("cf3"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoCClassAlias2(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("cf3"), "name1"),
        Alias(UnresolvedFieldReference("cf1"), "name2"),
        Alias(UnresolvedFieldReference("cf2"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojoAlias1(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("pf1"), "name1"),
        Alias(UnresolvedFieldReference("pf2"), "name2"),
        Alias(UnresolvedFieldReference("pf3"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(0, 1, 2)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoPojoAlias2(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("pf3"), "name1"),
        Alias(UnresolvedFieldReference("pf1"), "name2"),
        Alias(UnresolvedFieldReference("pf2"), "name3")
      ))

    tableSchema.getColumnNames.zip(Array("name1", "name2", "name3"))
        .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(2, 0, 1)).foreach(x => assertEquals(x._2, x._1))
  }

  @Test
  def testGetFieldInfoTimeAttribute(): Unit = {
    val tableSchema = TableSchema.fromTypeInfo(
      rowType,
      Array(
        RowtimeAttribute(UnresolvedFieldReference("f0")),
        UnresolvedFieldReference("f1"),
        UnresolvedFieldReference("f2"),
        ProctimeAttribute(UnresolvedFieldReference("f3"))
      ))

    tableSchema.getColumnNames.zip(Array("f0", "f1", "f2", "f3"))
      .foreach(x => assertEquals(x._2, x._1))
    tableSchema.getPhysicalIndices.zip(Array(-1, 1, 2, -2)).foreach(x => assertEquals(x._2, x._1))
  }
}
