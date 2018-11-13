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

import org.apache.flink.table.api.types.DataType;

/**
 * A GenericRow can have arbitrary number of fields and contain a set of fields, which may all be
 * different types. The fields in GenericRow can be null.
 *
 * <p>The fields in the Row can be accessed by position (zero-based) {@link #get(int, DataType)}.
 * And can update fields by {@link #update(int, Object)}.
 *
 * <p>GenericRow is in principle serializable. However, it may contain non-serializable fields,
 * in which case serialization will fail.
 */
public final class GenericRow extends ObjectArrayRow {

	public GenericRow(int arity) {
		super(arity);
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return (boolean) this.fields[ordinal];
	}

	@Override
	public byte getByte(int ordinal) {
		return (byte) this.fields[ordinal];
	}

	@Override
	public short getShort(int ordinal) {
		return (short) this.fields[ordinal];
	}

	@Override
	public int getInt(int ordinal) {
		return (int) this.fields[ordinal];
	}

	@Override
	public long getLong(int ordinal) {
		return (long) this.fields[ordinal];
	}

	@Override
	public float getFloat(int ordinal) {
		return (float) this.fields[ordinal];
	}

	@Override
	public double getDouble(int ordinal) {
		return (double) this.fields[ordinal];
	}

	@Override
	public char getChar(int ordinal) {
		return (char) this.fields[ordinal];
	}

	@Override
	public void setBoolean(int ordinal, boolean value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setByte(int ordinal, byte value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setShort(int ordinal, short value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setInt(int ordinal, int value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setLong(int ordinal, long value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setFloat(int ordinal, float value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setDouble(int ordinal, double value) {
		this.fields[ordinal] = value;
	}

	@Override
	public void setChar(int ordinal, char value) {
		this.fields[ordinal] = value;
	}

	public void update(int ordinal, Object value) {
		this.fields[ordinal] = value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof GenericRow)) {
			return false;
		}

		GenericRow row = (GenericRow) o;

		return getHeader() == row.getHeader() && equalObjectArray(fields, row.fields);
	}

	@Override
	public boolean equalsWithoutHeader(BaseRow o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof GenericRow)) {
			return false;
		}

		GenericRow row = (GenericRow) o;

		return equalObjectArray(fields, row.fields);
	}

	public Object getField(int ordinal) {
		return this.fields[ordinal];
	}

	public static GenericRow of(Object... values) {
		GenericRow row = new GenericRow(values.length);

		for (int i = 0; i < values.length; ++i) {
			row.update(i, values[i]);
		}

		return row;
	}

	public static GenericRow wrap(Object o) {
		GenericRow row = new GenericRow(1);
		row.update(0, o);
		return row;
	}

	public static GenericRow copyReference(GenericRow row) {
		final GenericRow newRow = new GenericRow(row.fields.length);
		System.arraycopy(row.fields, 0, newRow.fields, 0, row.fields.length);
		newRow.setHeader(row.getHeader());
		return newRow;
	}
}

