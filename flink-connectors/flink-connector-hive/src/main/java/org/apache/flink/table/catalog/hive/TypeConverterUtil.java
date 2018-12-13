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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.CharType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimeType;
import org.apache.flink.table.api.types.TimestampType;

import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Convert Hive data type to Blink data type.
 */
public class TypeConverterUtil {
	public static Map<TypeInformation, String> flinkTypeToHiveType = new HashMap<>();

	static {
		flinkTypeToHiveType.put(BasicTypeInfo.STRING_TYPE_INFO, serdeConstants.STRING_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.CHAR_TYPE_INFO, serdeConstants.CHAR_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, serdeConstants.BOOLEAN_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.BYTE_TYPE_INFO, serdeConstants.TINYINT_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.SHORT_TYPE_INFO, serdeConstants.SMALLINT_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.INT_TYPE_INFO, serdeConstants.INT_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.BIG_INT_TYPE_INFO, serdeConstants.BIGINT_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.FLOAT_TYPE_INFO, serdeConstants.FLOAT_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.DOUBLE_TYPE_INFO, serdeConstants.DOUBLE_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.DATE_TYPE_INFO, serdeConstants.DATE_TYPE_NAME);
		flinkTypeToHiveType.put(SqlTimeTypeInfo.TIME, serdeConstants.DATETIME_TYPE_NAME);
		flinkTypeToHiveType.put(SqlTimeTypeInfo.TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME);
		flinkTypeToHiveType.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, serdeConstants.DECIMAL_TYPE_NAME);
	}

	public static InternalType convert(String hiveType) {
		switch (hiveType) {
			case serdeConstants.STRING_TYPE_NAME:
				return StringType.INSTANCE;
			case serdeConstants.CHAR_TYPE_NAME:
				return CharType.INSTANCE;
			case serdeConstants.BOOLEAN_TYPE_NAME:
				return BooleanType.INSTANCE;
			case serdeConstants.TINYINT_TYPE_NAME:
				return ByteType.INSTANCE;
			case serdeConstants.SMALLINT_TYPE_NAME:
				return ShortType.INSTANCE;
			case serdeConstants.INT_TYPE_NAME:
				return IntType.INSTANCE;
			case serdeConstants.BIGINT_TYPE_NAME:
				return LongType.INSTANCE;
			case serdeConstants.FLOAT_TYPE_NAME:
				return FloatType.INSTANCE;
			case serdeConstants.DOUBLE_TYPE_NAME:
				return DoubleType.INSTANCE;
			case serdeConstants.DATE_TYPE_NAME:
				return DateType.DATE;
			case serdeConstants.DATETIME_TYPE_NAME:
				return TimeType.INSTANCE;
			case serdeConstants.TIMESTAMP_TYPE_NAME:
				return TimestampType.TIMESTAMP;
			case serdeConstants.DECIMAL_TYPE_NAME:
				return DecimalType.SYSTEM_DEFAULT;
			default:
				throw new UnsupportedOperationException(
					String.format("Flink doesn't support Hive's type %s yet.", hiveType));
		}
	}
}
