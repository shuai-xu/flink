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

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.BigDecimalTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.table.types.ArrayType;
import org.apache.flink.table.types.BaseRowType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.GenericType;
import org.apache.flink.table.types.MapType;
import org.apache.flink.table.types.TypeInfoWrappedType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TypeUtils;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * Provide type specialized getters and setters to reduce if/else and eliminate box and unbox.
 *
 * <p>There is only setter for the fixed-length type field, because the variable-length type
 * cannot be set to the binary format such as {@link BinaryRow}.
 */
public interface TypeGetterSetters {

	/**
	 * Get boolean value.
	 */
	boolean getBoolean(int ordinal);

	/**
	 * Get byte value.
	 */
	byte getByte(int ordinal);

	/**
	 * Get short value.
	 */
	short getShort(int ordinal);

	/**
	 * Get int value.
	 */
	int getInt(int ordinal);

	/**
	 * Get long value.
	 */
	long getLong(int ordinal);

	/**
	 * Get float value.
	 */
	float getFloat(int ordinal);

	/**
	 * Get double value.
	 */
	double getDouble(int ordinal);

	/**
	 * Get char value.
	 */
	char getChar(int ordinal);

	/**
	 * Get byte[] value.
	 */
	byte[] getByteArray(int ordinal);

	/**
	 * Get BinaryString(intenral String/Varchar) value.
	 */
	BinaryString getBinaryString(int ordinal);

	/**
	 * Get BinaryString(intenral String/Varchar) value by reuse object.
	 */
	BinaryString getBinaryString(int ordinal, BinaryString reuse);

	/**
	 * Get String value.
	 */
	default String getString(int ordinal) {
		return getBinaryString(ordinal).toString();
	}

	/**
	 * Get value of DECIMAL(precision,scale) from `ordinal`..
	 */
	Decimal getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get generic value by serializer.
	 */
	@Deprecated
	<T> T getGeneric(int ordinal, TypeSerializer<T> serializer);

	/**
	 * Get generic value by GenericType.
	 */
	<T> T getGeneric(int ordinal, GenericType<T> type);

	/**
	 * Get nested row value.
	 */
	BaseRow getBaseRow(int ordinal, int numFields);

	/**
	 * Get array value.
	 */
	BinaryArray getArray(int ordinal);

	/**
	 * Get map value.
	 */
	BinaryMap getMap(int ordinal);

	@Deprecated
	default Object get(int ordinal, TypeInformation type, TypeSerializer serializer) {
		if (type.equals(Types.BOOLEAN)) {
			return getBoolean(ordinal);
		} else if (type.equals(Types.BYTE)) {
			return getByte(ordinal);
		} else if (type.equals(Types.SHORT)) {
			return getShort(ordinal);
		} else if (type.equals(Types.INT)) {
			return getInt(ordinal);
		} else if (type.equals(Types.LONG)) {
			return getLong(ordinal);
		} else if (type.equals(Types.FLOAT)) {
			return getFloat(ordinal);
		} else if (type.equals(Types.DOUBLE)) {
			return getDouble(ordinal);
		} else if (type instanceof BigDecimalTypeInfo) {
			BigDecimalTypeInfo dt = (BigDecimalTypeInfo) type;
			return getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(Types.STRING)) {
			return getBinaryString(ordinal);
		} else if (type.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
			return getChar(ordinal);
		} else if (type.equals(TimeIndicatorTypeInfo.ROWTIME_INDICATOR())) {
			return getLong(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.DATE)) {
			return getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIME)) {
			return getInt(ordinal);
		} else if (type.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			return getLong(ordinal);
		} else if (type.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			return getByteArray(ordinal);
		} else if (TypeUtils.isInternalArrayType(type)) {
			return getArray(ordinal);
		} else if (type instanceof MapTypeInfo) {
			return getMap(ordinal);
		} else if (TypeUtils.isInternalCompositeType(type)) {
			return getBaseRow(ordinal, type.getArity());
		} else {
			return getGeneric(ordinal, serializer);
		}
	}

	default Object get(int ordinal, DataType type) {
		if (type.equals(DataTypes.BOOLEAN)) {
			return getBoolean(ordinal);
		} else if (type.equals(DataTypes.BYTE)) {
			return getByte(ordinal);
		} else if (type.equals(DataTypes.SHORT)) {
			return getShort(ordinal);
		} else if (type.equals(DataTypes.INT)) {
			return getInt(ordinal);
		} else if (type.equals(DataTypes.LONG)) {
			return getLong(ordinal);
		} else if (type.equals(DataTypes.FLOAT)) {
			return getFloat(ordinal);
		} else if (type.equals(DataTypes.DOUBLE)) {
			return getDouble(ordinal);
		} else if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			return getDecimal(ordinal, dt.precision(), dt.scale());
		} else if (type.equals(DataTypes.STRING)) {
			return getBinaryString(ordinal);
		} else if (type.equals(DataTypes.CHAR)) {
			return getChar(ordinal);
		} else if (type.equals(DataTypes.ROWTIME_INDICATOR)) {
			return getLong(ordinal);
		} else if (type.equals(DataTypes.DATE)) {
			return getInt(ordinal);
		} else if (type.equals(DataTypes.TIME)) {
			return getInt(ordinal);
		} else if (type.equals(DataTypes.TIMESTAMP)) {
			return getLong(ordinal);
		} else if (type.equals(DataTypes.BYTE_ARRAY)) {
			return getByteArray(ordinal);
		} else if (type instanceof ArrayType) {
			return getArray(ordinal);
		} else if (type instanceof MapType) {
			return getMap(ordinal);
		} else if (type instanceof BaseRowType) {
			return getBaseRow(ordinal, ((BaseRowType) type).getArity());
		} else if (type instanceof GenericType) {
			return getGeneric(ordinal, (GenericType) type);
		} else if (type instanceof TypeInfoWrappedType) {
			return get(ordinal, ((TypeInfoWrappedType) type).toInternalType());
		} else {
			throw new RuntimeException("Not support type: " + type);
		}
	}

	/**
	 * Set boolean value.
	 */
	void setBoolean(int ordinal, boolean value);

	/**
	 * Set byte value.
	 */
	void setByte(int ordinal, byte value);

	/**
	 * Set short value.
	 */
	void setShort(int ordinal, short value);

	/**
	 * Set int value.
	 */
	void setInt(int ordinal, int value);

	/**
	 * Set long value.
	 */
	void setLong(int ordinal, long value);

	/**
	 * Set float value.
	 */
	void setFloat(int ordinal, float value);

	/**
	 * Set double value.
	 */
	void setDouble(int ordinal, double value);

	/**
	 * Set char value.
	 */
	void setChar(int ordinal, char value);

	/**
	 * Set decimal value.
	 */
	void setDecimal(int ordinal, Decimal value, int precision, int scale);
}
