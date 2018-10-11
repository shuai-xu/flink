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

package org.apache.flink.table.descriptors

import java.util.Optional

import org.apache.flink.table.api.{TableException, TableSchema, Types}
import org.apache.flink.table.descriptors.RowtimeTest.CustomExtractor
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp}
import org.apache.flink.table.sources.wmstrategies.{BoundedOutOfOrderTimestamps, PreserveWatermarks}
import org.apache.flink.table.types.DataTypes
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Tests for [[SchemaValidator]].
  */
class SchemaValidatorTest {

  @Test
  def testSchemaWithRowtimeFromSource(): Unit = {
     val desc1 = Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING)
      .field("abcField", DataTypes.STRING)
      .field("myField", DataTypes.BOOLEAN)
      .build()

    // test proctime
    assertEquals(Optional.of("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[StreamRecordTimestamp])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "otherField" -> "csvField",
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField").asJava
    assertEquals(
      expectedMapping,
      SchemaValidator.deriveFieldMapping(props,
        Optional.of(DataTypes.toTypeInfo(inputSchema.toRowType))))

    // test field format
    val formatSchema = SchemaValidator.deriveFormatFields(props)
    val expectedFormatSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING) // aliased
      .field("abcField", DataTypes.STRING)
      .build()
    assertEquals(expectedFormatSchema, formatSchema)
  }

  @Test(expected = classOf[TableException])
  def testDeriveTableSinkSchemaWithRowtimeFromSource(): Unit = {
    val desc1 = Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromSource().watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    SchemaValidator.deriveTableSinkSchema(props)
  }

  @Test
  def testDeriveTableSinkSchemaWithRowtimeFromField(): Unit = {
    val desc1 = Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromField("myTime").watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val expectedTableSinkSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING) // aliased
      .field("abcField", DataTypes.STRING)
      .field("myTime", DataTypes.TIMESTAMP)
      .build()

    assertEquals(expectedTableSinkSchema, SchemaValidator.deriveTableSinkSchema(props))
  }

  @Test
  def testSchemaWithRowtimeFromField(): Unit = {
     val desc1 = Schema()
      .field("otherField", Types.STRING).from("csvField")
      .field("abcField", Types.STRING)
      .field("p", Types.SQL_TIMESTAMP).proctime()
      .field("r", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromField("myTime").watermarksFromSource())
    val props = new DescriptorProperties()
    desc1.addProperties(props)

    val inputSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING)
      .field("abcField", DataTypes.STRING)
      .field("myField", DataTypes.BOOLEAN)
      .field("myTime", DataTypes.TIMESTAMP)
      .build()

    // test proctime
    assertEquals(Optional.of("p"), SchemaValidator.deriveProctimeAttribute(props))

    // test rowtime
    val rowtime = SchemaValidator.deriveRowtimeAttributes(props).get(0)
    assertEquals("r", rowtime.getAttributeName)
    assertTrue(rowtime.getTimestampExtractor.isInstanceOf[ExistingField])
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[PreserveWatermarks])

    // test field mapping
    val expectedMapping = Map(
      "otherField" -> "csvField",
      "csvField" -> "csvField",
      "abcField" -> "abcField",
      "myField" -> "myField",
      "myTime" -> "myTime").asJava
    assertEquals(
      expectedMapping,
      SchemaValidator.deriveFieldMapping(props,
        Optional.of(DataTypes.toTypeInfo(inputSchema.toRowType))))

    // test field format
    val formatSchema = SchemaValidator.deriveFormatFields(props)
    val expectedFormatSchema = TableSchema.builder()
      .field("csvField", DataTypes.STRING) // aliased
      .field("abcField", DataTypes.STRING)
      .field("myTime", DataTypes.TIMESTAMP)
      .build()
    assertEquals(expectedFormatSchema, formatSchema)
  }

  @Test
  def testSchemaWithRowtimeCustomTimestampExtractor(): Unit = {
    val descriptor = Schema()
      .field("f1", Types.STRING)
      .field("f2", Types.STRING)
      .field("f3", Types.SQL_TIMESTAMP)
      .field("rt", Types.SQL_TIMESTAMP).rowtime(
        Rowtime().timestampsFromExtractor(new CustomExtractor("f3"))
          .watermarksPeriodicBounded(1000L))
    val properties = new DescriptorProperties()
    descriptor.addProperties(properties)

    val rowtime = SchemaValidator.deriveRowtimeAttributes(properties).get(0)
    assertEquals("rt", rowtime.getAttributeName)
    val extractor = rowtime.getTimestampExtractor
    assertTrue(extractor.equals(new CustomExtractor("f3")))
    assertTrue(rowtime.getWatermarkStrategy.isInstanceOf[BoundedOutOfOrderTimestamps])
  }
}
