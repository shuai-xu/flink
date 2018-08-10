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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.typeutils.BytewiseComparator;
import org.apache.flink.api.common.typeutils.SerializationException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.GroupOutOfRangeException;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.PrefixPartitionIterator;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.rocksdb.RocksIterator;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link InternalState} which is backed by
 * {@link RocksDBInternalStateBackend}.
 */
public class RocksDBInternalState implements InternalState {

	/**
	 *  Byte inserted before each column of key.
	 */
	static final byte KEY_PREFIX_BYTE = 0x0F;

	/**
	 *  Byte will not be inserted before each column of key, used to create prefix keys' end.
	 */
	static final byte KEY_END_BYTE = 0x7F;

	private static final int KEY = 0;

	private static final int VALUE = 1;

	/**
	 * The backend by which the state is backed.
	 */
	private final RocksDBInternalStateBackend backend;

	/**
	 * The descriptor of the state.
	 */
	private final InternalStateDescriptor descriptor;

	/** Number of bytes required to prefix the groups. */
	final byte[] stateNameBytes;

	/**
	 * The byte-wise comparator for RocksDB internal state.
	 */
	private final RocksDBKeyComparator defaultComparator;

	RocksDBInternalState(RocksDBInternalStateBackend backend, InternalStateDescriptor descriptor) {
		this.backend = Preconditions.checkNotNull(backend);
		this.descriptor = Preconditions.checkNotNull(descriptor);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			StringSerializer.INSTANCE.serialize(descriptor.getName(), new DataOutputViewStreamWrapper(out));
		} catch (IOException e) {
			throw new SerializationException(e);
		}
		this.stateNameBytes = out.toByteArray();
		this.defaultComparator = new RocksDBKeyComparator();
	}

	@Override
	public InternalStateDescriptor getDescriptor() {
		return descriptor;
	}

	@Override
	public int getNumGroups() {
		return backend.getNumGroups();
	}

	@Override
	public GroupSet getPartitionGroups() {
		return backend.getGroups();
	}

	@Nullable
	@Override
	public Row get(Row key) {
		if (key == null) {
			return null;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		int group = getGroupForKey(key);
		byte[] dbKey = serializeStateKey(group, key);
		byte[] dbValue = backend.getDbInstance().get(dbKey);
		if (dbValue == null) {
			return null;
		}

		List<Row> rowValues = deserializeStateValues(dbValue, this.descriptor);
		if (rowValues.size() > 1) {
			return mergeMultiValues(rowValues);
		} else {
			return rowValues.get(0);
		}
	}

	@Override
	public void put(Row key, Row value) {
		checkKeyAndValue(key, value);

		int group = getGroupForKey(key);
		byte[] dbKey = serializeStateKey(group, key);
		byte[] dbValue = serializeStateValue(value, descriptor);

		backend.getDbInstance().put(dbKey, dbValue);
	}

	@Override
	public void merge(Row key, Row value) {
		checkKeyAndValue(key, value);
		Preconditions.checkNotNull(descriptor.getValueMerger());

		int group = getGroupForKey(key);
		byte[] dbKey = serializeStateKey(group, key);

		byte[] mergedBytes = serializeStateValue(value, descriptor);
		backend.getDbInstance().merge(dbKey, mergedBytes);
	}

	@Override
	public void remove(Row key) {
		if (key == null) {
			return;
		}

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());

		int group = getGroupForKey(key);
		byte[] dbKey = serializeStateKey(group, key);

		backend.getDbInstance().delete(dbKey);
	}

	@Override
	public Map<Row, Row> getAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return Collections.emptyMap();
		}
		List<byte[]> keyBytesList = new ArrayList<>();
		Map<Row, byte[]> indexes = new HashMap<>(keys.size());

		Map<Row, Row> results = new HashMap<>(keys.size());

		for (Row key : keys) {
			if (key != null) {
				Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
				int group = descriptor.getPartitioner()
					.partition(key, backend.getNumGroups());

				byte[] dbKey = serializeStateKey(group, key);
				indexes.put(key, dbKey);
				keyBytesList.add(dbKey);
			}
		}

		Map<byte[], byte[]> rawResults = backend.getDbInstance().multiGet(keyBytesList);
		for (Map.Entry<Row, byte[]> index: indexes.entrySet()) {
			Row key = index.getKey();
			byte[] dbKey = index.getValue();
			byte[] dbValue = rawResults.get(dbKey);
			if (dbValue != null) {
				List<Row> rowValues = deserializeStateValues(dbValue, descriptor);
				Row value;
				if (rowValues.size() > 1) {
					value =  mergeMultiValues(rowValues);
				} else {
					value = rowValues.get(0);
				}
				results.put(key, value);
			}
		}

		return results;
	}

	@Override
	public void putAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		Map<byte[], byte[]> keyValueBytesMap = new HashMap<>(pairs.size());
		for (Map.Entry<Row, Row> pair : pairs.entrySet()) {
			Row key = pair.getKey();
			Row value = pair.getValue();
			checkKeyAndValue(key, value);

			int group = getGroupForKey(key);
			byte[] dbKey = serializeStateKey(group, key);
			byte[] dbValue = serializeStateValue(value, descriptor);
			keyValueBytesMap.put(dbKey, dbValue);
		}

		backend.getDbInstance().multiPut(keyValueBytesMap);
	}

	@Override
	public void mergeAll(Map<Row, Row> pairs) {
		if (pairs == null || pairs.isEmpty()) {
			return;
		}

		for (Map.Entry<Row, Row> entry : pairs.entrySet()) {
			merge(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void removeAll(Collection<Row> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}

		for (Row key : keys) {
			remove(key);
		}
	}

	@Override
	public Iterator<Pair<Row, Row>> iterator() {
		GroupSet groups = getPartitionGroups();

		int columnIndex = 0;
		if (isKeyColumnByteWiseOrdered(columnIndex)) {
			Collection<Iterator<RocksDBEntry>> groupIterators = new ArrayList<>();

			for (int group : groups) {
				byte[] groupBytes = serializePrefixKeys(group, null, null);
				byte[] groupByteEnd = serializePrefixKeysEnd(group, null);
				groupIterators.add(createRocksDBEntryRangeIterator(groupBytes, groupByteEnd));
			}

			return new RocksDBSortedPartitionIterator(groupIterators,
				defaultComparator,
				columnIndex);
		} else {
			Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();
			for (int group : groups) {
				byte[] groupBytes = serializePrefixKeys(group, null, null);
				byte[] groupByteEnd = serializePrefixKeysEnd(group, null);
				groupIterators.add(createRocksDBPairRangeIterator(groupBytes, groupByteEnd));
			}
			return new PrefixPartitionIterator(groupIterators);
		}
	}

	@Override
	public Iterator<Pair<Row, Row>> prefixIterator(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, null);
				byte[] prefixBytesEnd = serializePrefixKeysEnd(group, prefixKeys);

				return createRocksDBPairRangeIterator(prefixByteStart, prefixBytesEnd);
			}
		}

		GroupSet groups = getPartitionGroups();
		if (isKeyColumnByteWiseOrdered(numPrefixKeys)) {
			Collection<Iterator<RocksDBEntry>> groupIterators = new ArrayList<>();

			for (int group : groups) {
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, null);
				byte[] prefixBytesEnd = serializePrefixKeysEnd(group, prefixKeys);
				groupIterators.add(createRocksDBEntryRangeIterator(prefixByteStart, prefixBytesEnd));
			}

			return new RocksDBSortedPartitionIterator(groupIterators,
				defaultComparator,
				numPrefixKeys);
		} else {
			Collection<Iterator<Pair<Row, Row>>> groupIterators = new ArrayList<>();
			for (int group : groups) {
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, null);
				byte[] prefixBytesEnd = serializePrefixKeysEnd(group, prefixKeys);
				groupIterators.add(createRocksDBPairRangeIterator(prefixByteStart, prefixBytesEnd));
			}
			return new PrefixPartitionIterator(groupIterators);
		}
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKeys) {
		checkOrderedPrefixKeys(prefixKeys);

		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				return firstPair(group, prefixKeys);
			}
		}

		Comparator<?> keyComparator = getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator();
		Comparator<Object> comparator = keyComparator == null ? null : (Comparator<Object>) keyComparator;

		Pair<Row, Row> firstInternalPair = null;
		Object firstKey = null;
		for (int group : getPartitionGroups()) {
			Pair<Row, Row> groupFirstPair = firstPair(group, prefixKeys);
			if (groupFirstPair != null) {

				if (comparator == null) {
					return groupFirstPair;
				}

				Object groupFirstKey = groupFirstPair.getKey().getField(numPrefixKeys);
				if (firstKey == null || comparator.compare(firstKey, groupFirstKey) > 0) {
					firstInternalPair = groupFirstPair;
					firstKey = groupFirstKey;
				}
			}
		}
		return firstInternalPair;
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKeys) {
		checkOrderedPrefixKeys(prefixKeys);

		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				return lastPair(group, prefixKeys);
			}
		}

		Comparator<?> keyComparator = getDescriptor().getKeyColumnDescriptor(numPrefixKeys).getComparator();
		Comparator<Object> comparator = keyComparator == null ? null : (Comparator<Object>) keyComparator;

		Pair<Row, Row> lastInternalPair = null;
		Object lastKey = null;
		for (int group : getPartitionGroups()) {
			Pair<Row, Row> groupLastPair = lastPair(group, prefixKeys);
			if (groupLastPair != null) {

				if (comparator == null) {
					return groupLastPair;
				}

				Object groupLastKey = groupLastPair.getKey().getField(numPrefixKeys);
				if (lastKey == null || comparator.compare(lastKey, groupLastKey) < 0) {
					lastInternalPair = groupLastPair;
					lastKey = groupLastKey;
				}
			}
		}
		return lastInternalPair;
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> headIterator(Row prefixKeys, K endKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(isKeyColumnByteWiseOrdered(numPrefixKeys));

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, null);
				byte[] prefixBytesEnd = serializePrefixKeys(group, prefixKeys, endKey);

				return createRocksDBPairRangeIterator(prefixByteStart, prefixBytesEnd);
			}
		}

		Collection<Iterator<RocksDBEntry>> groupIterators = new ArrayList<>();

		for (int group : getPartitionGroups()) {
			groupIterators.add(headEntryIterator(group, prefixKeys, endKey));
		}

		return new RocksDBSortedPartitionIterator(groupIterators,
			defaultComparator,
			numPrefixKeys);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> tailIterator(Row prefixKeys, K startKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(isKeyColumnByteWiseOrdered(numPrefixKeys));

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, startKey);
				byte[] prefixBytesEnd = serializePrefixKeysEnd(group, prefixKeys);

				return createRocksDBPairRangeIterator(prefixByteStart, prefixBytesEnd);
			}
		}

		Collection<Iterator<RocksDBEntry>> groupIterators = new ArrayList<>();

		for (int group : getPartitionGroups()) {
			groupIterators.add(tailEntryIterator(group, prefixKeys, startKey));
		}

		return new RocksDBSortedPartitionIterator(groupIterators,
			defaultComparator,
			numPrefixKeys);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> subIterator(Row prefixKeys, K startKey, K endKey) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Preconditions.checkArgument(isKeyColumnByteWiseOrdered(numPrefixKeys));

		if (descriptor.getPartitioner() instanceof FieldBasedPartitioner) {
			FieldBasedPartitioner fieldBasedPartitioner = (FieldBasedPartitioner) descriptor.getPartitioner();

			if (fieldBasedPartitioner.getField() < numPrefixKeys) {
				int group = fieldBasedPartitioner.partition(prefixKeys, getNumGroups());
				byte[] prefixByteStart = serializePrefixKeys(group, prefixKeys, startKey);
				byte[] prefixBytesEnd = serializePrefixKeys(group, prefixKeys, endKey);

				return createRocksDBPairRangeIterator(prefixByteStart, prefixBytesEnd);
			}
		}

		Collection<Iterator<RocksDBEntry>> groupIterators = new ArrayList<>();

		for (int group : getPartitionGroups()) {
			groupIterators.add(subEntryIterator(group, prefixKeys, startKey, endKey));
		}

		return new RocksDBSortedPartitionIterator(groupIterators,
			defaultComparator,
			numPrefixKeys);
	}

	// ------------------------------------------------------------------------
	//  General utility method
	// ------------------------------------------------------------------------

	/**
	 * Get the group for the key.
	 *
	 * @param key The key to partition.
	 * @return The group for the key.
	 */
	private int getGroupForKey(Row key) {
		int groupsToPartition = backend.getNumGroups();

		return descriptor.getPartitioner()
			.partition(key, groupsToPartition);
	}

	// ------------------------------------------------------------------------
	//  Check whether arguments illegal
	// ------------------------------------------------------------------------

	private void checkOrderedPrefixKeys(Row prefixKeys) {
		int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();
		Preconditions.checkArgument(numPrefixKeys < descriptor.getNumKeyColumns());
		Comparator<?> comparator = descriptor.getKeyColumnDescriptor(numPrefixKeys).getComparator();
		Preconditions.checkArgument(comparator != null);
		Preconditions.checkArgument(comparator instanceof BytewiseComparator, "RocksDB internal state only supports byte-wise comparator currently.");
	}

	private void checkKeyAndValue(Row key, Row value) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(value);

		Preconditions.checkArgument(key.getArity() == descriptor.getNumKeyColumns());
		Preconditions.checkArgument(value.getArity() == descriptor.getNumValueColumns());
	}

	private void checkGroupRange(int group) {
		String stateName = getDescriptor().getName();
		GroupSet groups = backend.getGroups();
		if (!groups.contains(group)) {
			throw new GroupOutOfRangeException(stateName, groups, group);
		}
	}

	private boolean isKeyColumnByteWiseOrdered(int column) {
		Comparator<?> comparator = descriptor.getKeyColumnDescriptor(column).getComparator();
		if (comparator == null) {
			return false;
		} else if (!(comparator instanceof BytewiseComparator)) {
			throw new UnsupportedOperationException("RocksDB internal state only supports byte-wise comparator currently.");
		}
		return true;
	}

	// ------------------------------------------------------------------------
	//  Utility method with group information for internal APIs
	// ------------------------------------------------------------------------

	private Pair<Row, Row> firstPair(int group, Row prefixKeys) {
		checkGroupRange(group);

		Pair<Row, Row> result = null;

		byte[] startKeyBytes = serializePrefixKeys(group, prefixKeys, null);
		RocksDBInstance dbInstance = backend.getDbInstance();
		try (RocksIterator iterator = dbInstance.iterator()) {
			iterator.seek(startKeyBytes);
			if (iterator.isValid()) {
				byte[] dbKey = iterator.key();
				byte[] dbValue = iterator.value();

				byte[] dbKeyEnd = serializePrefixKeysEnd(group, prefixKeys);
				if (RocksDBInstance.compare(dbKey, dbKeyEnd) < 0) {
					result = (new RocksDBEntry(dbInstance, dbKey, dbValue)).getRowPair(descriptor);
				}
			}
		}
		return result;
	}

	private Pair<Row, Row> lastPair(int group, Row prefixKeys) {
		checkGroupRange(group);

		Pair<Row, Row> result = null;

		byte[] dbKeyEnd = serializePrefixKeysEnd(group, prefixKeys);
		RocksDBInstance dbInstance = backend.getDbInstance();

		try (RocksIterator iterator = dbInstance.iterator()) {
			iterator.seek(dbKeyEnd);

			if (iterator.isValid()) {
				iterator.prev();
			} else {
				iterator.seekToLast();
			}

			if (iterator.isValid()) {
				byte[] dbKey = iterator.key();
				byte[] dbValue = iterator.value();

				byte[] dbKeyStart = serializePrefixKeys(group, prefixKeys, null);
				if (RocksDBInstance.compare(dbKeyStart, dbKey) < 0) {
					result = (new RocksDBEntry(dbInstance, dbKey, dbValue)).getRowPair(descriptor);
				}
			}
		}
		return result;
	}

	private  <K> Iterator<RocksDBEntry> headEntryIterator(int group, Row prefixKeys, K endKey) {
		checkGroupRange(group);

		byte[] startKeyBytes = serializePrefixKeys(group, prefixKeys, null);
		byte[] endKeyBytes = serializePrefixKeys(group, prefixKeys, endKey);
		return createRocksDBEntryRangeIterator(startKeyBytes, endKeyBytes);
	}

	private  <K> Iterator<RocksDBEntry> tailEntryIterator(int group, Row prefixKeys, K startKey) {
		checkGroupRange(group);

		byte[] startKeyBytes = serializePrefixKeys(group, prefixKeys, startKey);
		byte[] endKeyBytes = serializePrefixKeysEnd(group, prefixKeys);
		return createRocksDBEntryRangeIterator(startKeyBytes, endKeyBytes);
	}

	private  <K> Iterator<RocksDBEntry> subEntryIterator(int group, Row prefixKeys, K startKey, K endKey) {
		checkGroupRange(group);

		byte[] startKeyBytes = serializePrefixKeys(group, prefixKeys, startKey);
		byte[] endKeyBytes = serializePrefixKeys(group, prefixKeys, endKey);
		return createRocksDBEntryRangeIterator(startKeyBytes, endKeyBytes);
	}

	private Iterator<Pair<Row, Row>> createRocksDBPairRangeIterator(byte[] startKeyBytes, byte[] endKeyBytes) {
		final RocksDBInstance dbInstance = backend.getDbInstance();
		return
			new RocksDBStateRangeIterator<Pair<Row, Row>>(dbInstance, startKeyBytes, endKeyBytes) {
				@Override
				public Pair<Row, Row> next() {
					return getNextEntry().getRowPair(descriptor);
				}
			};
	}

	private Iterator<RocksDBEntry> createRocksDBEntryRangeIterator(byte[] startKeyBytes, byte[] endKeyBytes) {
		final RocksDBInstance dbInstance = backend.getDbInstance();
		return
			new RocksDBStateRangeIterator<RocksDBEntry>(dbInstance, startKeyBytes, endKeyBytes) {
				@Override
				public RocksDBEntry next() {
					return getNextEntry();
				}
			};
	}

	// ------------------------------------------------------------------------
	//  Utility method for serialization/ deserialization
	// ------------------------------------------------------------------------

	private byte[] serializeStateKey(int group, Row key) {
		try {
			ByteArrayOutputStreamWithPosition outputStream = new ByteArrayOutputStreamWithPosition();

			outputStream.writeInt(group);
			outputStream.write(stateNameBytes);

			serializeRow(key, outputStream, descriptor.getKeySerializer(), KEY);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	static Row deserializeStateKey(byte[] bytes, InternalStateDescriptor descriptor) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			inputView.skipBytesToRead(IntSerializer.INSTANCE.getLength());
			StringSerializer.INSTANCE.deserialize(inputView);

			return deserializeRow(descriptor, inputView, KEY);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	static byte[] serializeStateValue(Row value, InternalStateDescriptor descriptor) {
		if (value == null) {
			return null;
		}
		try {
			ByteArrayOutputStreamWithPosition outputStream = new ByteArrayOutputStreamWithPosition();

			serializeRow(value, outputStream, descriptor.getValueSerializer(), VALUE);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	static List<Row> deserializeStateValues(byte[] bytes, InternalStateDescriptor descriptor) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			return deserializeValueRows(descriptor, inputView);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	private Row mergeMultiValues(List<Row> rowValues) {
		return mergeMultiValues(rowValues, descriptor.getValueMerger());
	}

	static Row mergeMultiValues(List<Row> rowValues, Merger<Row> merger) {
		Preconditions.checkNotNull(merger);
		return rowValues.stream().reduce(merger::merge).get();
	}

	static Row deserializeStateValue(byte[] bytes, InternalStateDescriptor descriptor) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			return deserializeRow(descriptor, inputView, VALUE);
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	private static void serializeRow(Row row, ByteArrayOutputStreamWithPosition outputStream, RowSerializer rowSerializer, int flag) throws IOException {
		int len = row.getArity();

		DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);
		for (int i = 0; i < len; i++) {
			Object filed = row.getField(i);
			if (flag == KEY) {
				outputStream.write(KEY_PREFIX_BYTE);
			}
			if (filed != null) {
				outputView.writeBoolean(false);
				TypeSerializer<Object> serializer = (TypeSerializer<Object>) rowSerializer.getFieldSerializers()[i];
				if (flag == KEY) {
					if (serializer.getLength() > 0) {
						outputView.writeInt(serializer.getLength());
						serializer.serialize(filed, outputView);
					} else {
						int lengthPos = outputStream.getPosition();
						outputView.writeInt(-1);
						serializer.serialize(filed, outputView);
						int currentPos = outputStream.getPosition();
						writeIntAtPosition(currentPos - lengthPos - 4, outputStream, lengthPos);
					}
				} else {
					serializer.serialize(filed, outputView);
				}
			} else {
				outputView.writeBoolean(true);
			}
		}
	}

	private static void writeIntAtPosition(int v, ByteArrayOutputStreamWithPosition out, int position) throws IOException {
		out.writeByteAtPos((v >>> 24) & 0xFF, position);
		out.writeByteAtPos((v >>> 16) & 0xFF, position + 1);
		out.writeByteAtPos((v >>>  8) & 0xFF, position + 2);
		out.writeByteAtPos((v >>>  0) & 0xFF, position + 3);
	}

	static Row deserializeRow(
		InternalStateDescriptor descriptor,
		DataInputViewStreamWrapper inputView,
		int flag) throws IOException {

		int len = flag == KEY ? descriptor.getNumKeyColumns() : descriptor.getNumValueColumns();
		RowSerializer rowSerializer = flag == KEY ? descriptor.getKeySerializer() : descriptor.getValueSerializer();

		Row result = new Row(len);
		for (int i = 0; i < len; i++) {
			if (flag == KEY) {
				inputView.skipBytesToRead(1);
			}
			boolean isNullField = inputView.readBoolean();
			if (isNullField) {
				result.setField(i, null);
			} else {
				TypeSerializer<Object> serializer = (TypeSerializer<Object>) rowSerializer.getFieldSerializers()[i];
				if (flag == KEY) {
					inputView.skipBytesToRead(4);
				}
				result.setField(i, serializer.deserialize(inputView));
			}
		}

		return result;
	}

	static List<Row> deserializeValueRows(
		InternalStateDescriptor descriptor,
		DataInputViewStreamWrapper inputView) throws IOException {

		List<Row> rows = new ArrayList<>();
		int len = descriptor.getNumValueColumns();
		RowSerializer rowSerializer = descriptor.getValueSerializer();

		// We should check whether input view has been merged.
		do {
			Row result = new Row(len);
			for (int i = 0; i < len; i++) {
				boolean isNullField = inputView.readBoolean();
				if (isNullField) {
					result.setField(i, null);
				} else {
					TypeSerializer<Object> serializer = (TypeSerializer<Object>) rowSerializer.getFieldSerializers()[i];
					result.setField(i, serializer.deserialize(inputView));
				}
			}
			rows.add(result);
		} while (inputView.available() > 0 &&
			inputView.read() == RocksDBInternalStateBackend.DELIMITER);

		return rows;
	}

	/**
	 * Serialize group, prefixKeys and start key.
	 * Before calling this method, we must check arity of prefixKeys less than num of key columns
	 *
	 * @param group The group of this state.
	 * @param prefixKeys The prefix keys to be iterator.
	 * @param nextKey The next key succeeding the
	 *                 given prefix in the rows to be iterated over.
	 * @param <K> Type of the keys succeeding the prefix.
	 * @return The serialization of the group, prefixKeys and the next start key.
	 */
	private <K> byte[] serializePrefixKeys(int group, Row prefixKeys, K nextKey) {
		try {
			ByteArrayOutputStreamWithPosition outputStream = new ByteArrayOutputStreamWithPosition();
			int numPrefixKeys = prefixKeys == null ? 0 : prefixKeys.getArity();

			outputStream.writeInt(group);
			outputStream.write(stateNameBytes);

			// We should add null masks to output view.
			if (prefixKeys != null) {
				serializeRow(prefixKeys, outputStream, descriptor.getKeySerializer(), KEY);
			}
			if (nextKey != null) {
				serializeRow(Row.of(nextKey), outputStream,
					new RowSerializer(new TypeSerializer[] {descriptor.getKeyColumnDescriptor(numPrefixKeys).getSerializer()}), KEY);
			}
			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	private byte[] serializePrefixKeysEnd(int group, Row prefixKeys) {
		try {
			ByteArrayOutputStreamWithPosition outputStream = new ByteArrayOutputStreamWithPosition();

			outputStream.writeInt(group);
			outputStream.write(stateNameBytes);

			// We should add null masks to output view.
			if (prefixKeys != null) {
				serializeRow(prefixKeys, outputStream, descriptor.getKeySerializer(), KEY);
			}
			outputStream.write(KEY_END_BYTE);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new SerializationException(e);
		}
	}

	/**
	 * The sorted iterator over the values in RocksDB internal state for different groups.
	 */
	private class RocksDBSortedPartitionIterator extends AbstractRocksDBSortedPartitionIterator<Pair<Row, Row>> {

		private RocksDBKeyComparator comparator;

		private int keyField;

		private RocksDBSortedPartitionIterator(Collection<Iterator<RocksDBEntry>> groupIterators,
											RocksDBKeyComparator comparator,
											int keyField) {
			super(groupIterators, RocksDBInternalState.this.descriptor);

			this.comparator = Preconditions.checkNotNull(comparator);
			this.keyField = Preconditions.checkNotNull(keyField);
		}

		@Override
		protected int compareKeys(byte[] keyA, byte[] keyB) {
			return comparator.compare(keyA, keyB, keyField);
		}

		@Override
		protected Pair<Row, Row> getValueFromPair(Pair<Row, Row> pair) {
			return pair;
		}
	}

	/**
	 * Helper class which could write byte at specific position of internal byte buffer.
	 */
	private static class ByteArrayOutputStreamWithPosition extends org.apache.flink.core.memory.ByteArrayOutputStreamWithPos {

		void writeByteAtPos(int b, int position) {
			Preconditions.checkArgument(position <= count, "Position out of bound.");
			buffer[position] = (byte) b;
		}

		void writeInt(int v) {
			write((v >>> 24) & 0xFF);
			write((v >>> 16) & 0xFF);
			write((v >>> 8) & 0xFF);
			write((v >>> 0) & 0xFF);
		}
	}

	/**
	 * Bytes comparator for {@link RocksDBInternalState}'s key.
	 */
	private class RocksDBKeyComparator {

		/**
		 * Prefix bytes before key: (int) group, (String) stateName.
		 */
		private int prefixLength;

		private RowSerializer rowSerializer;

		RocksDBKeyComparator() {
			this.prefixLength = IntSerializer.INSTANCE.getLength() + stateNameBytes.length;
			this.rowSerializer = descriptor.getKeySerializer();
		}

		int compare(byte[] bytesA, byte[] bytesB, int field) {
			ByteArrayInputStreamWithPos inputStreamA = new ByteArrayInputStreamWithPos(bytesA, prefixLength, bytesA.length - prefixLength);
			DataInputViewStreamWrapper inputViewA = new DataInputViewStreamWrapper(inputStreamA);

			ByteArrayInputStreamWithPos inputStreamB = new ByteArrayInputStreamWithPos(bytesB, prefixLength, bytesB.length - prefixLength);
			DataInputViewStreamWrapper inputViewB = new DataInputViewStreamWrapper(inputStreamB);

			skipFirstToFiledKeys(inputViewA, field);
			skipFirstToFiledKeys(inputViewB, field);

			try {
				// skip KEY flag
				inputViewA.skipBytesToRead(1);

				// skip KEY flag
				inputViewB.skipBytesToRead(1);
			} catch (IOException e) {
				throw new SerializationException(e);
			}

			return RocksDBInstance.compare(getFieldBytes(inputViewA, field), getFieldBytes(inputViewB, field));
		}

		private byte[] getFieldBytes(DataInputViewStreamWrapper inputView, int field) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(baos);
			try {
				rowSerializer.getFieldSerializers()[field].copy(inputView, outputView);
			} catch (IOException e) {
				throw new SerializationException(e);
			}

			return baos.toByteArray();
		}

		private void skipFirstToFiledKeys(DataInputViewStreamWrapper inputView, int filed) {
			for (int i = 0; i < filed; i++) {
				try {
					// skip KEY flag
					inputView.skipBytesToRead(1);
					boolean isNullField = inputView.readBoolean();
					if (!isNullField) {
						int bytesToSkip = inputView.readInt();
						inputView.skipBytesToRead(bytesToSkip);
					}
				} catch (IOException e) {
					throw new SerializationException(e);
				}
			}
		}
	}
}
