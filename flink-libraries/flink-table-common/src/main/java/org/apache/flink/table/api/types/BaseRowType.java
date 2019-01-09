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

package org.apache.flink.table.api.types;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Row type for baseRow.
 *
 * <p>It's internal data structure is {@link BaseRow}, and it's external data structure is {@link Row}.
 */
public class BaseRowType extends InternalType {

	/**
	 * Internal type class to BaseRow.
	 */
	private final Class<? extends BaseRow> internalTypeClass;

	/**
	 * Use DataType instead of InternalType to convert to Row (if a Pojo in Row).
	 */
	private final DataType[] types;

	private final String[] fieldNames;

	/**
	 * Default useBaseRow is false, user use Row in Source/Sink/Udx.
	 */
	private final boolean useBaseRow;

	public BaseRowType(DataType... types) {
		this(BaseRow.class, types, getFieldNames(types.length));
	}

	public BaseRowType(Class<? extends BaseRow> internalTypeClass, DataType... types) {
		this(internalTypeClass, types, getFieldNames(types.length));
	}

	public BaseRowType(DataType[] types, String[] fieldNames) {
		this(BaseRow.class, types, fieldNames);
	}

	public BaseRowType(Class<? extends BaseRow> internalTypeClass, DataType[] types, String[] fieldNames) {
		this(internalTypeClass, types, fieldNames, false);
	}

	public BaseRowType(Class<? extends BaseRow> internalTypeClass, DataType[] types, boolean useBaseRow) {
		this(internalTypeClass, types, getFieldNames(types.length), useBaseRow);
	}

	public BaseRowType(Class<? extends BaseRow> internalTypeClass, DataType[] types, String[] fieldNames, boolean useBaseRow) {
		this.internalTypeClass = internalTypeClass;
		this.types = types;
		this.fieldNames = fieldNames;
		this.useBaseRow = useBaseRow;
	}

	private static String[] getFieldNames(int length) {
		String[] fieldNames = new String[length];
		for (int i = 0; i < length; i++) {
			fieldNames[i] = "f" + i;
		}
		return fieldNames;
	}

	public int getArity() {
		return types.length;
	}

	public Class<? extends BaseRow> getInternalTypeClass() {
		return internalTypeClass;
	}

	public DataType[] getFieldTypes() {
		return types;
	}

	public InternalType[] getFieldInternalTypes() {
		return Arrays.stream(types).map(DataType::toInternalType).toArray(InternalType[]::new);
	}

	public InternalType getInternalTypeAt(int i) {
		return types[i].toInternalType();
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	public boolean isUseBaseRow() {
		return useBaseRow;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BaseRowType that = (BaseRowType) o;

		return internalTypeClass.equals(that.internalTypeClass) &&
				Arrays.equals(getFieldInternalTypes(), that.getFieldInternalTypes()) &&
				Arrays.equals(fieldNames, that.fieldNames);
	}

	@Override
	public int hashCode() {
		int result = internalTypeClass.hashCode();
		result = 31 * result + Arrays.hashCode(types);
		result = 31 * result + Arrays.hashCode(fieldNames);
		return result;
	}

	@Override
	public String toString() {
		return "BaseRowType{" +
				"internalTypeClass=" + internalTypeClass +
				", types=" + Arrays.toString(types) +
				", fieldNames=" + Arrays.toString(fieldNames) +
				", useBaseRow=" + useBaseRow +
				'}';
	}
}
