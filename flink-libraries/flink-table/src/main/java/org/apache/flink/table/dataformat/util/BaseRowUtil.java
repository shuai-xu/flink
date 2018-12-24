/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.api.types.ArrayType;
import org.apache.flink.table.api.types.BaseRowType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.MapType;
import org.apache.flink.table.api.types.TypeInfoWrappedType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Util for base row.
 */
public final class BaseRowUtil {

	/**
	 * Indicates the row as an accumulate message.
	 */
	public static final byte ACCUMULATE_MSG = 0;

	/**
	 * Indicates the row as a retraction message.
	 */
	public static final byte RETRACT_MSG = 1;

	public static boolean isAccumulateMsg(BaseRow baseRow) {
		return baseRow.getHeader() == ACCUMULATE_MSG;
	}

	public static boolean isRetractMsg(BaseRow baseRow) {
		return baseRow.getHeader() == RETRACT_MSG;
	}

	public static BaseRow setAccumulate(BaseRow baseRow) {
		baseRow.setHeader(ACCUMULATE_MSG);
		return baseRow;
	}

	public static BaseRow setRetract(BaseRow baseRow) {
		baseRow.setHeader(RETRACT_MSG);
		return baseRow;
	}

	public static GenericRow toGenericRow(
		BaseRow baseRow,
		TypeInformation[] typeInfos,
		TypeSerializer[] typeSerializers) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.update(i, null);
				} else {
					row.update(i, get(baseRow, i, typeInfos[i], typeSerializers[i]));
				}
			}
			return row;
		}
	}

	public static GenericRow toGenericRow(
			BaseRow baseRow,
			InternalType[] types) {
		if (baseRow instanceof GenericRow) {
			return (GenericRow) baseRow;
		} else {
			GenericRow row = new GenericRow(baseRow.getArity());
			row.setHeader(baseRow.getHeader());
			for (int i = 0; i < row.getArity(); i++) {
				if (baseRow.isNullAt(i)) {
					row.update(i, null);
				} else {
					row.update(i, baseRow.get(i, types[i]));
				}
			}
			return row;
		}
	}

	public static Object get(BaseRow row, int ordinal, TypeInformation type, TypeSerializer serializer) {
		if (type.equals(Types.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(Types.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(Types.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(Types.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(Types.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(Types.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(Types.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type instanceof BigDecimalTypeInfo) {
			BigDecimalTypeInfo dt = (BigDecimalTypeInfo) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(Types.STRING)) {
			return row.getBinaryString(ordinal);
		} else if (type.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
			return row.getChar(ordinal);
		} else if (type.equals(TimeIndicatorTypeInfo.ROWTIME_INDICATOR())) {
			return row.getLong(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			return row.getByteArray(ordinal);
		} else if (TypeUtils.isInternalArrayType(type)) {
			return row.getBaseArray(ordinal);
		} else if (type instanceof MapTypeInfo) {
			return row.getBaseMap(ordinal);
		} else if (TypeUtils.isInternalCompositeType(type)) {
			return row.getBaseRow(ordinal, type.getArity());
		} else {
			return row.getGeneric(ordinal, serializer);
		}
	}

	public static Object get(BaseRow row, int ordinal, DataType type) {
		if (type.equals(org.apache.flink.table.api.types.Types.BOOLEAN)) {
			return row.getBoolean(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.BYTE)) {
			return row.getByte(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.SHORT)) {
			return row.getShort(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.INT)) {
			return row.getInt(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.LONG)) {
			return row.getLong(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.FLOAT)) {
			return row.getFloat(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.DOUBLE)) {
			return row.getDouble(ordinal);
		} else if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			return row.getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(org.apache.flink.table.api.types.Types.STRING)) {
			return row.getBinaryString(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.CHAR)) {
			return row.getChar(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.ROWTIME_INDICATOR)) {
			return row.getLong(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.DATE)) {
			return row.getInt(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.TIME)) {
			return row.getInt(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.TIMESTAMP)) {
			return row.getLong(ordinal);
		} else if (type.equals(org.apache.flink.table.api.types.Types.BYTE_ARRAY)) {
			return row.getByteArray(ordinal);
		} else if (type instanceof ArrayType) {
			return row.getBaseArray(ordinal);
		} else if (type instanceof MapType) {
			return row.getBaseMap(ordinal);
		} else if (type instanceof BaseRowType) {
			return row.getBaseRow(ordinal, ((BaseRowType) type).getArity());
		} else if (type instanceof GenericType) {
			return row.getGeneric(ordinal, (GenericType) type);
		} else if (type instanceof TypeInfoWrappedType) {
			return row.get(ordinal, ((TypeInfoWrappedType) type).toInternalType());
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}

	public static String toOriginString(BaseRow row, TypeInformation[] types, TypeSerializer[] serializers) {
		checkArgument(types.length == row.getArity());
		StringBuilder build = new StringBuilder("[");
		build.append(row.getHeader());
		for (int i = 0; i < row.getArity(); i++) {
			build.append(',');
			if (row.isNullAt(i)) {
				build.append("null");
			} else {
				TypeSerializer serializer = serializers != null ? serializers[i] : null;
				build.append(get(row, i, types[i], serializer));
			}
		}
		build.append(']');
		return build.toString();
	}
}
