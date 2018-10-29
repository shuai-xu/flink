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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BoxedValue;

import java.io.IOException;

/**
 * Serializer for {@link BoxedValue}.
 */
@Internal
public final class BoxedValueSerializer<T> extends TypeSerializer<BoxedValue<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> valueSerializer;

	public BoxedValueSerializer(TypeSerializer<T> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BoxedValue<T>> duplicate() {
		return new BoxedValueSerializer<>(valueSerializer.duplicate());
	}

	@Override
	public BoxedValue<T> createInstance() {
		return new BoxedValue<>(valueSerializer.createInstance());
	}

	@Override
	public BoxedValue<T> copy(BoxedValue<T> from) {
		return new BoxedValue<>(valueSerializer.copy(from.getValue()));
	}

	@Override
	public BoxedValue<T> copy(BoxedValue<T> from, BoxedValue<T> reuse) {
		reuse.setValue(valueSerializer.copy(from.getValue(), reuse.getValue()));
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BoxedValue<T> record, DataOutputView target) throws IOException {
		T value = record.getValue();
		if (value == null) {
			// write null flag
			target.writeBoolean(true);
		} else {
			target.writeBoolean(false);
			valueSerializer.serialize(value, target);
		}
	}

	@Override
	public BoxedValue<T> deserialize(DataInputView source) throws IOException {
		boolean isNull = source.readBoolean();
		if (isNull) {
			return new BoxedValue<>();
		} else {
			T value = valueSerializer.deserialize(source);
			return new BoxedValue<>(value);
		}
	}

	@Override
	public BoxedValue<T> deserialize(BoxedValue<T> record, DataInputView source) throws IOException {
		boolean isNull = source.readBoolean();
		if (isNull) {
			record.setValue(null);
		} else {
			T value = record.getValue();
			if (value == null) {
				value = valueSerializer.deserialize(source);
			} else {
				value = valueSerializer.deserialize(value, source);
			}
			record.setValue(value);
		}
		return record;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		target.write(bytes);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BoxedValueSerializer) {
			BoxedValueSerializer other = (BoxedValueSerializer) obj;
			return this.valueSerializer.equals(other.valueSerializer);
		}
		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BoxedValueSerializer;
	}

	@Override
	public int hashCode() {
		return valueSerializer.hashCode();
	}

	private String serializationFormatIdentifier; // lazy
	private String getSerializationFormatIdentifier() {
		String id = serializationFormatIdentifier;
		if (id == null) {
			id = getClass().getCanonicalName() + "," + valueSerializer.getClass().getCanonicalName();
			serializationFormatIdentifier = id;
		}
		return id;
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
	}

	@Override
	public CompatibilityResult<BoxedValue<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ParameterlessTypeSerializerConfig
			&& isCompatibleSerializationFormatIdentifier(
			((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	private boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return identifier.equals(getSerializationFormatIdentifier());
	}

}
