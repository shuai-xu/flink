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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base row type info.
 */
public class BaseRowTypeInfo<T extends BaseRow> extends TupleTypeInfoBase<T> {

	private static final long serialVersionUID = 1L;

	protected final String[] fieldNames;

	public BaseRowTypeInfo(Class<T> rowType, TypeInformation<?>... types) {
		this(rowType, types, getFieldNames(types));
	}

	private static String[] getFieldNames(TypeInformation<?>[] types) {
		String[] fieldNames = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			fieldNames[i] = "f" + i;
		}
		return fieldNames;
	}

	public BaseRowTypeInfo(Class<T> rowType, TypeInformation<?>[] types, String[] fieldNames) {
		super(rowType, types);
		checkNotNull(fieldNames, "FieldNames should not be null.");
		checkArgument(
			types.length == fieldNames.length,
			"Number of field types and names is different.");
		checkArgument(
			!hasDuplicateFieldNames(fieldNames),
			"Field names are not unique.");
		this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public TypeComparator<T> createComparator(
		int[] logicalKeyFields,
		boolean[] orders,
		int logicalFieldOffset,
		ExecutionConfig config) {
		return (TypeComparator) new BaseRowComparator(types, orders[0]);
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseRowTypeInfo;
	}

	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}

	@Override
	public String toString() {
		StringBuilder bld = new StringBuilder("Row");
		if (types.length > 0) {
			bld.append('(').append(fieldNames[0]).append(": ").append(types[0]);

			for (int i = 1; i < types.length; i++) {
				bld.append(", ").append(fieldNames[i]).append(": ").append(types[i]);
			}

			bld.append(')');
		}
		return bld.toString();
	}

	/**
	 * Returns the field types of the row. The order matches the order of the field names.
	 */
	public TypeInformation<?>[] getFieldTypes() {
		return types;
	}

	private boolean hasDuplicateFieldNames(String[] fieldNames) {
		HashSet<String> names = new HashSet<>();
		for (String field : fieldNames) {
			if (!names.add(field)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public CompositeType.TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return createSerializer();
	}

	public AbstractRowSerializer<T> createSerializer() {
		if (getTypeClass() == BinaryRow.class) {
			return (AbstractRowSerializer<T>) new BinaryRowSerializer(types);
		} else {
			return new BaseRowSerializer<>(getTypeClass(), types);
		}
	}

	public RowType toInternalType() {
		return (RowType) DataTypes.internal(this);
	}

	/**
	 * UnionTransform will invoke this to BinaryRowTypeInfo,
	 * so we need override it to not compare type class.
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BaseRowTypeInfo) {
			BaseRowTypeInfo other = (BaseRowTypeInfo) obj;
			return Arrays.equals(this.types, other.types);
		}
		return false;
	}
}
