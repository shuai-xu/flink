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

package org.apache.flink.table.sources.parquet

import java.io.File
import java.sql.Timestamp

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.batch.sql.QueryTest
import org.apache.flink.table.runtime.conversion.InternalTypeConverters
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions.toLong
import org.apache.flink.table.sinks.parquet.RowParquetOutputFormat
import org.apache.flink.table.util.DateTimeTestUtil.UTCTimestamp
import org.apache.flink.types.Row
import org.junit.Test

// tests we borrowed from spark

class ParquetTableSourceITCase2 extends QueryTest {

  @Test // Read Parquet file generated by parquet-thrift
  def testThriftCompatibility(): Unit = {

    val parquetFile =
      "src/test/resources/test-data/parquet/parquet-thrift-compat.snappy.parquet"

    /* --- schema ---
      required boolean boolColumn;
      required int32 byteColumn;
      required int32 shortColumn;
      required int32 intColumn;
      required int64 longColumn;
      required double doubleColumn;
      required binary binaryColumn (UTF8);
      required binary stringColumn (UTF8);
      required binary enumColumn (ENUM);
      optional boolean maybeBoolColumn;
      optional int32 maybeByteColumn;
      optional int32 maybeShortColumn;
      optional int32 maybeIntColumn;
      optional int64 maybeLongColumn;
      optional double maybeDoubleColumn;
      optional binary maybeBinaryColumn (UTF8);
      optional binary maybeStringColumn (UTF8);
      optional binary maybeEnumColumn (ENUM);
      ... other complex types
     */
    // NOTE: we do not support complex parquet types (List etc)

    // bool ... bool ...
    val types = {
      val t1 = Array[InternalType](
        DataTypes.BOOLEAN,
        DataTypes.BYTE,
        DataTypes.SHORT,
        DataTypes.INT,
        DataTypes.LONG,
        DataTypes.DOUBLE,
        DataTypes.STRING,
        DataTypes.STRING,
        DataTypes.STRING)

      t1 ++ t1
    }

    // boolColumn ... maybeBoolColumn ...
    val names = {
      val ns = Array("bool", "byte", "short", "int", "long", "double", "binary", "string", "enum")
      ns.map(_ + "Column") ++ ns.map("maybe" + _.capitalize + "Column")
    }

    val values: Seq[Row] = (0 until 10).map { i =>
      val suits = Array("SPADES", "HEARTS", "DIAMONDS", "CLUBS")

      val nonNullablePrimitiveValues = Seq(
        i % 2 == 0,
        i.toByte,
        (i + 1).toShort,
        i + 2,
        i.toLong * 10,
        i.toDouble + 0.2d,
        // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
        // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
        // Thrift `STRING`s are encoded using UTF-8.
        s"val_$i",
        s"val_$i",
        // Thrift ENUM values are converted to Parquet binaries containing UTF-8 strings
        suits(i % 4))

      val nullablePrimitiveValues = if (i % 3 == 0) {
        Seq.fill(nonNullablePrimitiveValues.length)(null)
      } else {
        nonNullablePrimitiveValues
      }
      val row = (nonNullablePrimitiveValues ++ nullablePrimitiveValues).map(_.asInstanceOf[AnyRef])
      Row.of(row: _*)
    }

    val tableSource =
      new ParquetVectorizedColumnRowTableSource(
        new Path(parquetFile), types, names, true)

    tEnv.registerTableSource("ttt", tableSource)

    val results = tEnv.sqlQuery("select * from ttt").collect()

    org.junit.Assert.assertTrue(values == results)
    // we could read binaryColumn as byte[].
    // but `==` would fail (it does not compare byte[] contents)

  }


  // -- query parquet data -----------------------------------------------------

  def checkQuery(
      schema: Seq[(String, InternalType)],
      tableData: Seq[Product],
      sqlQuery: String,
      expected: Seq[Product],
      tableName: String = "t",
      resultTypes: Array[InternalType] = null)
    : Unit = {

    val colNames: Array[String] = schema.map(_._1).toArray
    val colTypes: Array[InternalType] = schema.map(_._2).toArray

    val toRow = (p: Product) =>
      GenericRow.of(p.productIterator.map(_.asInstanceOf[AnyRef]).toArray: _*)

    val tempFile = File.createTempFile("parquet-test", ".parquet")
    tempFile.delete()
    val outFormat = new RowParquetOutputFormat(tempFile.getAbsolutePath, colTypes, colNames)
    outFormat.open(1, 1)
    tableData.map(toRow).foreach(outFormat.writeRecord)
    outFormat.close()
    tempFile.deleteOnExit()
    val parquetFile = tempFile.getAbsolutePath

    val tableSource =
      new ParquetVectorizedColumnRowTableSource(
        new Path(parquetFile), colTypes, colNames, true)

    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    tEnv.registerTableSource(tableName, tableSource)

    val results = tEnv.sqlQuery(sqlQuery).collect()

    val types = if (resultTypes == null) colTypes else resultTypes
    val converter = InternalTypeConverters.createToExternalConverter(
      DataTypes.of(new RowTypeInfo(types.map(DataTypes.toTypeInfo): _*)))
    org.junit.Assert.assertTrue(expected.map(toRow).map(converter) == results)
  }


  @Test
  def testSimpleSelect(): Unit = {
    val schema = Seq("_1" -> DataTypes.INT, "_2" -> DataTypes.STRING)
    val tableData = (0 until 10).map(i => (i, i.toString))
    val query = "select * from t"
    val expected = tableData

    checkQuery(schema, tableData, query, expected)
  }

  @Test
  def testSelfJoin(): Unit = {
    val schema = Seq("_1" -> DataTypes.INT, "_2" -> DataTypes.STRING)
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    val tableData = (1 to 4).map { i =>
      val maybeInt = if (i % 2 == 0) null: Integer else i: Integer
      (maybeInt, i.toString)
    }
    val query = "SELECT * FROM t x JOIN t y ON x._1 = y._1"
    val expected = Seq((1, "1", 1, "1"), (3, "3", 3, "3"))

    checkQuery(schema, tableData, query, expected,
      resultTypes = Array(DataTypes.INT, DataTypes.STRING, DataTypes.INT, DataTypes.STRING))
  }

  @Test
  def testFilter(): Unit = {
    val schema = Seq("_1" -> DataTypes.INT, "_2" -> DataTypes.STRING)
    val tableData = (1 to 10).map(i => (i, i.toString))
    val query = "select * from t where _1<10"
    val expected = (1 to 9).map(i => (i, i.toString))

    checkQuery(schema, tableData, query, expected)
  }

  @Test // strings stored using dictionary compression in parquet
  def testStringCompressed(): Unit = {
    val schema = Seq("_1" -> DataTypes.STRING, "_2" -> DataTypes.STRING, "_3" -> DataTypes.INT)
    val tableData = (0 until 1000).map(i => ("same", "run_" + i / 100, 1))

    {
      val query = "SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2 order by _2"
      val expected = (0 until 10).map(i => ("same", "run_" + i, 100))
      checkQuery(schema, tableData, query, expected)
    }

    {
      val query = "SELECT _1, _2, SUM(_3) FROM t WHERE _2 = 'run_5' GROUP BY _1, _2"
      val expected = Seq(("same", "run_5", 100))
      checkQuery(schema, tableData, query, expected)
    }
  }

  @Test
  def testSqlTimestamp(): Unit = {
    val schema = Seq("_1" -> DataTypes.INT, "_2" -> DataTypes.TIMESTAMP)
    val tableData = (1 to 10).map(i => (i, toLong(new Timestamp(i))))
    val query = "select * from t"
    val expected = tableData

    checkQuery(schema, tableData, query, expected)
  }

  @Test // timestamp as int64, only precise to ms
  def testSqlTimestampTruncation(): Unit = {
    val schema = Seq("_1" -> DataTypes.INT, "_2" -> DataTypes.TIMESTAMP)
    val tableData = Seq((1, toLong(UTCTimestamp("2001-01-01 01:01:01.001001001"))))
    val query = "select * from t"
    val expected = Seq((1, toLong(UTCTimestamp("2001-01-01 01:01:01.001"))))

    checkQuery(schema, tableData, query, expected)
  }

}
