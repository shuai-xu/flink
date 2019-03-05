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

package org.apache.flink.table.sinks.filesystem

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{BIG_DEC_TYPE_INFO, BYTE_TYPE_INFO, DOUBLE_TYPE_INFO, FLOAT_TYPE_INFO, INT_TYPE_INFO, LONG_TYPE_INFO, SHORT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO
import org.apache.flink.table.api.Types
import org.apache.flink.table.dataformat.{BinaryString, Decimal}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.binaryRow
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions
import org.apache.flink.table.types.{DataTypes, TypeConverters}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.DateTimeTestUtil.{UTCDate, UTCTime, UTCTimestamp}

import org.junit.Test

import java.util.TimeZone

/** Test for PartitionPathUtils. **/
class PartitionPathUtilsTest {

  @Test
  def testGetPartitionPath(): Unit = {
    val rowTypeInfo = new BaseRowTypeInfo(
      BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
      STRING_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      INT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      BIG_DEC_TYPE_INFO,
      Types.SQL_TIME,
      Types.SQL_DATE,
      Types.SQL_TIMESTAMP)
    val row = binaryRow(rowTypeInfo,
      "abc d".getBytes,
      BinaryString.fromString("abc%_*\n\t"),
      "a".getBytes.head,
      1.shortValue(),
      23,
      456789102L,
      1.23f,
      4.56D,
      Decimal.castFrom(3404045.5044003, 38, 18),
      BuildInScalarFunctions.toInt(UTCTime("5:23:43 +08:00")),
      BuildInScalarFunctions.toInt(UTCDate("2019-02-21")),
      BuildInScalarFunctions.toLong(UTCTimestamp("2019-02-21 13:47:56")))
    val partitionPath = PartitionPathUtils.getPartitionPath(row, DataTypes.createRowTypeV2(
      rowTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
      rowTypeInfo.getFieldNames), TimeZone.getTimeZone("UTC"))
    val expectPath = "f0=abc d/" +
      "f1=abc%25_%2A%0A%09/" +
      "f2=97/" +
      "f3=1/" +
      "f4=23/" +
      "f5=456789102/" +
      "f6=1.23/" +
      "f7=4.56/" +
      "f8=3404045.504400300000000000/" +
      "f9=13%3A23%3A43/" +
      "f10=2019-02-21/" +
      "f11=2019-02-21 13%3A47%3A56.000"
    assert(partitionPath == expectPath)
  }

}
