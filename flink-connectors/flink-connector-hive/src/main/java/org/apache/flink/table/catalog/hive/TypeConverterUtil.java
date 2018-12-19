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
}
