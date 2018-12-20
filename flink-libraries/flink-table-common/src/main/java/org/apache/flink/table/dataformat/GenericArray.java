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

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.Types;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;

/**
 * A GenericArray is an array where all the elements have the same type.
 * It can be considered as a wrapper class of the normal java array.
 *
 * <p>Primitive type elements are stored in primitive type java arrays,
 * other type elements are stored in Object java arrays.</p>
 *
 * <p>Note that boxed type (Integer, Long, etc.) are not considered to be primitive types.</p>
 */
public class GenericArray extends BaseArray {

	private final Object arr;
	private final boolean[] isNull;

	private final int numElements;
	private final boolean isPrimitive;

	public GenericArray(int numElements, boolean isPrimitive, InternalType elementType) {
		if (isPrimitive) {
			if (elementType.equals(Types.BOOLEAN)) {
				arr = new boolean[numElements];
			} else if (elementType.equals(Types.BYTE)) {
				arr = new byte[numElements];
			} else if (elementType.equals(Types.CHAR)) {
				arr = new char[numElements];
			} else if (elementType.equals(Types.SHORT)) {
				arr = new short[numElements];
			} else if (elementType.equals(Types.INT)) {
				arr = new int[numElements];
			} else if (elementType.equals(Types.LONG)) {
				arr = new long[numElements];
			} else if (elementType.equals(Types.FLOAT)) {
				arr = new float[numElements];
			} else if (elementType.equals(Types.DOUBLE)) {
				arr = new double[numElements];
			} else {
				throw new RuntimeException("Element type is not primitive");
			}
			isNull = null;
		} else {
			arr = new Object[numElements];
			isNull = new boolean[numElements];
		}

		this.numElements = numElements;
		this.isPrimitive = isPrimitive;
	}

	public GenericArray(Object arr, int numElements, boolean isPrimitive) {
		this.arr = arr;
		isNull = isPrimitive ? null : new boolean[numElements];

		this.numElements = numElements;
		this.isPrimitive = isPrimitive;
	}

	@Override
	public int numElements() {
		return numElements;
	}

	@Override
	public boolean isNullAt(int pos) {
		return !isPrimitive && isNull[pos];
	}

	@Override
	public void setNullAt(int pos) {
		Preconditions.checkState(!isPrimitive, "Can't set null for primitive array");

		isNull[pos] = true;
		((Object[]) arr)[pos] = null;
	}

	@Override
	public void setNotNullAt(int pos) {
		if (!isPrimitive) {
			isNull[pos] = false;
		}
	}

	@Override
	public void setNullLong(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullInt(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullBoolean(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullByte(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullShort(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullFloat(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullDouble(int pos) {
		setNullAt(pos);
	}

	@Override
	public void setNullChar(int pos) {
		setNullAt(pos);
	}

	@Override
	public boolean[] toBooleanArray() {
		return (boolean[]) arr;
	}

	@Override
	public byte[] toByteArray() {
		return (byte[]) arr;
	}

	@Override
	public short[] toShortArray() {
		return (short[]) arr;
	}

	@Override
	public int[] toIntArray() {
		return (int[]) arr;
	}

	@Override
	public long[] toLongArray() {
		return (long[]) arr;
	}

	@Override
	public float[] toFloatArray() {
		return (float[]) arr;
	}

	@Override
	public double[] toDoubleArray() {
		return (double[]) arr;
	}

	@Override
	public Object[] toObjectArray(InternalType elementType) {
		return (Object[]) arr;
	}

	@Override
	public boolean getBoolean(int pos) {
		return ((boolean[]) arr)[pos];
	}

	@Override
	public byte getByte(int pos) {
		return ((byte[]) arr)[pos];
	}

	@Override
	public short getShort(int pos) {
		return ((short[]) arr)[pos];
	}

	@Override
	public int getInt(int pos) {
		return ((int[]) arr)[pos];
	}

	@Override
	public long getLong(int pos) {
		return ((long[]) arr)[pos];
	}

	@Override
	public float getFloat(int pos) {
		return ((float[]) arr)[pos];
	}

	@Override
	public double getDouble(int pos) {
		return ((double[]) arr)[pos];
	}

	@Override
	public char getChar(int pos) {
		return ((char[]) arr)[pos];
	}

	@Override
	public byte[] getByteArray(int pos) {
		return (byte[]) ((Object[]) arr)[pos];
	}

	@Override
	public String getString(int pos) {
		Object value = ((Object[]) arr)[pos];
		if (value instanceof BinaryString) {
			return value.toString();
		} else {
			return (String) value;
		}
	}

	@Override
	public BinaryString getBinaryString(int pos) {
		Object value = ((Object[]) arr)[pos];
		if (value instanceof BinaryString) {
			return (BinaryString) value;
		} else {
			return BinaryString.fromString((String) value);
		}
	}

	@Override
	public BinaryString getBinaryString(int pos, BinaryString reuseRef) {
		return getBinaryString(pos);
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		Object value = ((Object[]) arr)[pos];
		if (value instanceof Decimal) {
			return (Decimal) value;
		} else {
			return Decimal.fromBigDecimal((BigDecimal) value, precision, scale);
		}
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		return (T) ((Object[]) arr)[pos];
	}

	@Override
	public <T> T getGeneric(int pos, GenericType<T> type) {
		return (T) ((Object[]) arr)[pos];
	}

	@Override
	public BaseRow getBaseRow(int pos, int numFields) {
		return (BaseRow) ((Object[]) arr)[pos];
	}

	@Override
	public BaseArray getBaseArray(int pos) {
		return (BaseArray) ((Object[]) arr)[pos];
	}

	@Override
	public BaseMap getBaseMap(int pos) {
		return (BaseMap) ((Object[]) arr)[pos];
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		((boolean[]) arr)[pos] = value;
	}

	@Override
	public void setByte(int pos, byte value) {
		((byte[]) arr)[pos] = value;
	}

	@Override
	public void setShort(int pos, short value) {
		((short[]) arr)[pos] = value;
	}

	@Override
	public void setInt(int pos, int value) {
		((int[]) arr)[pos] = value;
	}

	@Override
	public void setLong(int pos, long value) {
		((long[]) arr)[pos] = value;
	}

	@Override
	public void setFloat(int pos, float value) {
		((float[]) arr)[pos] = value;
	}

	@Override
	public void setDouble(int pos, double value) {
		((double[]) arr)[pos] = value;
	}

	@Override
	public void setChar(int pos, char value) {
		((char[]) arr)[pos] = value;
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision, int scale) {
		((Object[]) arr)[pos] = value;
	}

	public void setObject(int pos, Object value) {
		((Object[]) arr)[pos] = value;
	}
}
