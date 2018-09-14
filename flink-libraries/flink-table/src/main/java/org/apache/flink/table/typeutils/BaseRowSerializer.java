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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.codegen.CodeGenUtils;
import org.apache.flink.table.codegen.CodeGeneratorContext;
import org.apache.flink.table.codegen.GeneratedProjection;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BoxedWrapperRow;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.types.BaseRowType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.InternalType;

import java.io.IOException;
import java.util.Arrays;

/**
 * Serializer for BaseRow.
 */
public class BaseRowSerializer<T extends BaseRow> extends AbstractRowSerializer<T> {

	private final GeneratedProjection gProjection;
	protected BinaryRowSerializer binarySerializer;
	private Class<T> rowType;

	private transient Projection<BaseRow, BinaryRow> projection;

	public BaseRowSerializer(TypeInformation<?>... types) {
		this((Class) BaseRow.class, types);
	}

	public BaseRowSerializer(InternalType... types) {
		this((Class) BaseRow.class, toTypeInfos(types));
	}

	private static TypeInformation[] toTypeInfos(InternalType... types) {
		TypeInformation[] typeInfos = new TypeInformation[types.length];
		for (int i = 0; i < typeInfos.length; i++) {
			typeInfos[i] = DataTypes.toTypeInfo(types[i]);
		}
		return typeInfos;
	}

	public BaseRowSerializer(Class<T> rowType, TypeInformation<?>... types) {
		super(types);
		this.rowType = rowType;
		this.binarySerializer = new BinaryRowSerializer(types);
		this.gProjection = genProjection(types);
	}

	public static GeneratedProjection genProjection(TypeInformation[] types) {
		BaseRowTypeInfo baseType = new BaseRowTypeInfo(BaseRow.class, types);
		int[] mapping = new int[types.length];
		for (int i = 0; i < types.length; i++) {
			mapping[i] = i;
		}
		return ProjectionCodeGenerator.generateProjection(
				CodeGeneratorContext.apply(new TableConfig(), false), "BaseRowSerializerProjection",
				(BaseRowType) DataTypes.internal(baseType),
				(BaseRowType) DataTypes.internal(new BaseRowTypeInfo<>(BinaryRow.class, types)),
				mapping);
	}

	@SuppressWarnings("unchecked")
	public Projection<BaseRow, BinaryRow> getProjection() throws IOException {
		if (projection == null) {
			try {
				projection = (Projection<BaseRow, BinaryRow>) CodeGenUtils.compile(
						// currentThread must be user class loader.
						Thread.currentThread().getContextClassLoader(),
						gProjection.name(), gProjection.code()).newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		return projection;
	}

	@Override
	public TypeSerializer<T> duplicate() {
		return new BaseRowSerializer<>(rowType, types);
	}

	@Override
	public T createInstance() {
		if (rowType.equals(GenericRow.class)) {
			//noinspection unchecked
			return (T) new GenericRow(getNumFields());
		} else if (rowType.equals(BoxedWrapperRow.class)) {
			//noinspection unchecked
			return (T) new BoxedWrapperRow(getNumFields());
		} else if (rowType.equals(ColumnarRow.class)) {
			//noinspection unchecked
			return (T) new ColumnarRow();
		} else {
			// default use binary row to deserializer
			//noinspection unchecked
			return (T) new BinaryRow(getNumFields());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public T copy(T from) {
		return (T) from.copy();
	}

	@Override
	public T copy(T from, T reuse) {
		//noinspection unchecked
		return (T) from.copy(reuse);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	public BinaryRow baseRowToBinary(BaseRow baseRow) throws IOException {
		BinaryRow row = getProjection().apply(baseRow);
		row.setHeader(baseRow.getHeader());
		return row;
	}

	@Override
	public void serialize(BaseRow row, DataOutputView target) throws IOException {
		BinaryRow binaryRow;
		if (row.getClass() == BinaryRow.class) {
			binaryRow = (BinaryRow) row;
		} else {
			binaryRow = baseRowToBinary(row);
		}
		binarySerializer.serialize(binaryRow, target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		//noinspection unchecked
		return (T) binarySerializer.deserialize(source);
	}

	@Override
	public T deserialize(BaseRow reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryRow) {
			//noinspection unchecked
			return (T) binarySerializer.deserialize((BinaryRow) reuse, source);
		} else {
			//noinspection unchecked
			return (T) binarySerializer.deserialize(source);
		}
	}

	@Override
	public int serializeToPages(BaseRow row, AbstractPagedOutputView target) throws IOException {
		return binarySerializer.serializeToPages(baseRowToBinary(row), target);
	}

	@Override
	public T deserializeFromPages(AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public T deserializeFromPages(T reuse,
			AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public T mapFromPages(AbstractPagedInputView source) throws IOException {
		//noinspection unchecked
		return (T) binarySerializer.mapFromPages(source);
	}

	@Override
	public T mapFromPages(T reuse,
			AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			BaseRowSerializer other = (BaseRowSerializer) obj;
			return Arrays.equals(types, other.types);
		}

		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseRowSerializer;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	private BinaryRowSerializer createBinaryRowSerializer() {
		return new BinaryRowSerializer(getTypes());
	}
}
