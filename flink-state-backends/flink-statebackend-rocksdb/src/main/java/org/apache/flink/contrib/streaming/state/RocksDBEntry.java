/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.contrib.streaming.state.RocksDBInternalState.GROUP_BYTES_TO_SKIP;

/**
 * The rocksDB entry of (dbKey, dbValue).
 */
public class RocksDBEntry {

	private final RocksDBInstance dbInstance;

	private final byte[] dbKey;

	private byte[] dbValue;

	private boolean deleted;

	RocksDBEntry(final RocksDBInstance dbInstance, final byte[] dbKey, final byte[] dbValue) {
		Preconditions.checkArgument(dbInstance != null);
		Preconditions.checkArgument(dbKey != null);
		Preconditions.checkArgument(dbValue != null);

		this.dbInstance = dbInstance;
		this.dbKey = dbKey;
		this.dbValue = dbValue;
		this.deleted = false;
	}

	void remove() {
		Preconditions.checkState(!deleted);

		deleted = true;
		dbValue = null;
		dbInstance.delete(dbKey);
	}

	/**
	 * Get the pair of row with given internal state descriptor.
	 *
	 * @param descriptor the given internal state descriptor
	 */
	Pair<Row, Row> getRowPair(InternalStateDescriptor descriptor, int prefixKeyLength) {
		Preconditions.checkNotNull(descriptor,
			"Must provide internal state descriptor to get the pair of row");

		return new Pair<Row, Row>() {
			/** The key of the pair. */
			private Row key;

			/** The value of the pair. */
			private Row value;

			@Override
			public Row getKey() {
				if (key == null) {
					key = deserializeStateKey(dbKey, descriptor, prefixKeyLength);
				}
				return key;
			}

			@Override
			public Row getValue() {
				if (dbValue == null) {
					return null;
				}
				if (value == null) {
					if (descriptor.getValueMerger() != null) {
						List<Row> rows = RocksDBInternalState.deserializeStateValues(dbValue, descriptor);
						value = RocksDBInternalState.mergeMultiValues(rows, descriptor.getValueMerger());
					} else {
						value = RocksDBInternalState.deserializeStateValue(dbValue, descriptor);
					}
				}
				return value;
			}

			@Override
			public Row setValue(Row value) {
				Preconditions.checkNotNull(value);
				Row oldValue = getValue();
				byte[] valueBytes = RocksDBInternalState.serializeStateValue(value, descriptor);
				dbInstance.put(dbKey, valueBytes);
				this.value = value;
				return oldValue;
			}
		};
	}

	private Row deserializeStateKey(byte[] bytes, InternalStateDescriptor descriptor, int prefixKeyLength) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			inputView.skipBytesToRead(prefixKeyLength);

			return RocksDBInternalState.deserializeKeyRow(descriptor, inputView);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	/**
	 * Get the pair of row with given internal state descriptor and duplicated key/value serializers.
	 * This method is used within async full snapshot procedure.
	 *
	 * @param descriptor the given internal state descriptor.
	 * @param dupKeySerializer the duplicated key serializer.
	 * @param dupValueSerializer the duplicated value serializer.
	 */
	Pair<Row, Row> getRowPair(
		InternalStateDescriptor descriptor,
		RowSerializer dupKeySerializer,
		RowSerializer dupValueSerializer) {

		Preconditions.checkNotNull(descriptor,
			"Must provide internal state descriptor to get the pair of row");
		Preconditions.checkNotNull(dupKeySerializer, "Must provide keySerializer to get the pair of row.");
		Preconditions.checkNotNull(dupValueSerializer, "Must provide valueSerializer to get the pair of row.");

		return new Pair<Row, Row>() {
			/** The key of the pair. */
			private Row key;

			/** The value of the pair. */
			private Row value;

			@Override
			public Row getKey() {
				if (key == null) {
					key = deserializeStateKey(dbKey, descriptor, dupKeySerializer);
				}
				return key;
			}

			@Override
			public Row getValue() {
				if (dbValue == null) {
					return null;
				}
				if (value == null) {
					value = deserializeStateValue(dbValue, descriptor, dupValueSerializer);
				}
				return value;
			}

			@Override
			public Row setValue(Row value) {
				Preconditions.checkNotNull(value);
				Row oldValue = getValue();
				byte[] valueBytes = serializeStateValue(value, dupValueSerializer);
				dbInstance.put(dbKey, valueBytes);
				this.value = value;
				return oldValue;
			}
		};
	}

	private Row deserializeStateKey(byte[] dbKey, InternalStateDescriptor descriptor, RowSerializer dupKeySerializer) {
		int numKeyColumns = descriptor.getNumKeyColumns();
		Row result = new Row(numKeyColumns);
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(dbKey);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			inputView.skipBytesToRead(GROUP_BYTES_TO_SKIP);
			StringSerializer.INSTANCE.deserialize(inputView);
			for (int i = 0; i < numKeyColumns; i++) {
				inputView.skipBytesToRead(1);
				boolean isNullField = inputView.readBoolean();
				if (isNullField) {
					result.setField(i, null);
				} else {
					TypeSerializer<Object> serializer = (TypeSerializer<Object>) dupKeySerializer.getFieldSerializers()[i];
					result.setField(i, serializer.deserialize(inputView));
				}
			}
		} catch (IOException e) {
			throw new SerializationException(e);
		}

		return result;
	}

	private Row deserializeStateValue(byte[] dbValue, InternalStateDescriptor descriptor, RowSerializer dupValueSerializer) {
		int numValueColumns = descriptor.getNumValueColumns();

		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(dbValue);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			List<Row> rows = new ArrayList<>();

			// We should check whether input view has been merged.
			do {
				Row result = new Row(numValueColumns);
				for (int i = 0; i < numValueColumns; i++) {
					boolean isNullField = inputView.readBoolean();
					if (isNullField) {
						result.setField(i, null);
					} else {
						TypeSerializer<Object> serializer = (TypeSerializer<Object>) dupValueSerializer.getFieldSerializers()[i];
						result.setField(i, serializer.deserialize(inputView));
					}
				}
				rows.add(result);
			} while (inputView.available() > 0 &&
				inputView.read() == RocksDBInternalStateBackend.DELIMITER);

			if (rows.size() > 1) {
				Merger<Row> merger = Preconditions.checkNotNull(descriptor.getValueMerger());
				return RocksDBInternalState.mergeMultiValues(rows, merger);
			} else {
				return rows.get(0);
			}
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	private byte[] serializeStateValue(Row value, RowSerializer dupValueSerializer) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();

			int len = value.getArity();

			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
			for (int i = 0; i < len; i++) {
				Object filed = value.getField(i);
				if (filed != null) {
					outputView.writeBoolean(false);
					TypeSerializer<Object> serializer = (TypeSerializer<Object>) dupValueSerializer.getFieldSerializers()[i];
					serializer.serialize(filed, outputView);
				} else {
					outputView.writeBoolean(true);
				}
			}

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	byte[] getDBKey() {
		return dbKey;
	}

	byte[] getDBValue() {
		return dbValue;
	}

	boolean isDeleted() {
		return deleted;
	}
}
