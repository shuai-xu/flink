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

package org.apache.flink.table.types;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.TypeUtils;

import java.util.Arrays;

/**
 * Row type for baseRow.
 */
public class BaseRowType implements InternalType {

	private final Class<?> typeClass;
	private final InternalType[] types;
	private final String[] fieldNames;

	private BaseRowSerializer serializer;

	public BaseRowType(InternalType... types) {
		this(BaseRow.class, types, getFieldNames(types.length));
	}

	public BaseRowType(Class<?> typeClass, InternalType... types) {
		this(typeClass, types, getFieldNames(types.length));
	}

	public BaseRowType(InternalType[] types, String[] fieldNames) {
		this(BaseRow.class, types, fieldNames);
	}

	public BaseRowType(Class<?> typeClass, InternalType[] types, String[] fieldNames) {
		this.typeClass = typeClass;
		this.types = types;
		this.fieldNames = fieldNames;
	}

	public BaseRowSerializer getSerializer() {
		if (serializer == null) {
			this.serializer = (BaseRowSerializer) TypeUtils.createSerializer(this);
		}
		return serializer;
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

	public boolean isBinary() {
		return typeClass == BinaryRow.class;
	}

	public Class<?> getTypeClass() {
		return typeClass;
	}

	public InternalType[] getFieldTypes() {
		return types;
	}

	public InternalType getTypeAt(int i) {
		return types[i];
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BaseRowType that = (BaseRowType) o;

		return typeClass.equals(that.typeClass) &&
				Arrays.equals(types, that.types) &&
				Arrays.equals(fieldNames, that.fieldNames);
	}

	@Override
	public int hashCode() {
		int result = typeClass.hashCode();
		result = 31 * result + Arrays.hashCode(types);
		result = 31 * result + Arrays.hashCode(fieldNames);
		return result;
	}
}
