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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RowMerger;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The descriptor for {@link InternalState}s.
 */
public final class InternalStateDescriptor implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the state.
	 */
	private final String name;

	/**
	 * The partition method for the keys in the state.
	 */
	private final Partitioner<Row> partitioner;

	/**
	 * The descriptors of the key columns in the state.
	 */
	private final List<InternalColumnDescriptor<?>> keyColumnDescriptors;

	/**
	 * The descriptors of the value columns in the state.
	 */
	private final List<InternalColumnDescriptor<?>> valueColumnDescriptors;

	/**
	 * Constructor with given name, scope, the partition method and the
	 * descriptors for the columns.
	 *
	 * @param name The name of the state.
	 * @param partitioner The partition method for the keys in the state.
	 * @param keyColumnDescriptors The descriptors of the key columns.
	 * @param valueColumnDescriptors The descriptors of the value columns.
	 */
	public InternalStateDescriptor(
		String name,
		Partitioner<Row> partitioner,
		List<InternalColumnDescriptor<?>> keyColumnDescriptors,
		List<InternalColumnDescriptor<?>> valueColumnDescriptors
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(partitioner);

		Map<String, InternalColumnDescriptor> declaredColumns = new HashMap<>();

		Preconditions.checkNotNull(keyColumnDescriptors);
		Preconditions.checkArgument(!keyColumnDescriptors.isEmpty());
		for (InternalColumnDescriptor<?> keyColumnDescriptor : keyColumnDescriptors) {
			Preconditions.checkNotNull(keyColumnDescriptor);
			Preconditions.checkArgument(!declaredColumns.containsKey(keyColumnDescriptor.getName()));

			declaredColumns.put(keyColumnDescriptor.getName(), keyColumnDescriptor);
		}

		Preconditions.checkNotNull(valueColumnDescriptors);
		Preconditions.checkArgument(!valueColumnDescriptors.isEmpty());
		for (InternalColumnDescriptor<?> valueColumnDescriptor : valueColumnDescriptors) {
			Preconditions.checkNotNull(valueColumnDescriptor);
			Preconditions.checkArgument(!declaredColumns.containsKey(valueColumnDescriptor.getName()));

			declaredColumns.put(valueColumnDescriptor.getName(), valueColumnDescriptor);
		}

		this.name = name;
		this.partitioner = partitioner;
		this.keyColumnDescriptors = new ArrayList<>(keyColumnDescriptors);
		this.valueColumnDescriptors = new ArrayList<>(valueColumnDescriptors);
	}

	/**
	 * Returns the name of the state.
	 *
	 * @return The name of the state.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the partition method for the keys in the state.
	 *
	 * @return The partition method for the keys in the state.
	 */
	public Partitioner<Row> getPartitioner() {
		return partitioner;
	}

	/**
	 * Returns the descriptors for the key columns in the state.
	 *
	 * @return The descriptors for the key columns in the state.
	 */
	public List<InternalColumnDescriptor<?>> getKeyColumnDescriptors() {
		return Collections.unmodifiableList(keyColumnDescriptors);
	}

	/**
	 * Returns the descriptors for the value columns in the state.
	 *
	 * @return The descriptors for the value columns in the state.
	 */
	public List<InternalColumnDescriptor<?>> getValueColumnDescriptors() {
		return Collections.unmodifiableList(valueColumnDescriptors);
	}

	/**
	 * Returns the number of the key columns in the state.
	 *
	 * @return The number of the key columns in the state.
	 */
	public int getNumKeyColumns() {
		return keyColumnDescriptors.size();
	}

	/**
	 * Returns the number of the value columns in the state.
	 *
	 * @return The number of the value columns in the state.
	 */
	public int getNumValueColumns() {
		return valueColumnDescriptors.size();
	}

	/**
	 * Returns the descriptor for the key column at the given position.
	 *
	 * @param index The index of the key column whose descriptor is to be
	 *              retrieved.
	 * @return The descriptor for the key column at the given position.
	 */
	public InternalColumnDescriptor<?> getKeyColumnDescriptor(int index) {
		return keyColumnDescriptors.get(index);
	}

	/**
	 * Returns the descriptor for the value column at given position.
	 *
	 * @param index The index of the value column whose descriptor is to be
	 *              retrieved.
	 * @return The descriptor for the value column at the given position.
	 */
	public InternalColumnDescriptor<?> getValueColumnDescriptor(int index) {
		return valueColumnDescriptors.get(index);
	}

	/**
	 * Returns the serializer for the keys in the state.
	 *
	 * @return The serializer for the keys in the state.
	 */
	public RowSerializer getKeySerializer() {
		TypeSerializer<?>[] keyColumnSerializers =
			new TypeSerializer<?>[getNumKeyColumns()];

		for (int index = 0; index < getNumKeyColumns(); ++index) {
			keyColumnSerializers[index] =
					getKeyColumnDescriptor(index).getSerializer();
		}

		return new RowSerializer(keyColumnSerializers);
	}

	/**
	 * Returns the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public RowSerializer getValueSerializer() {
		TypeSerializer<?>[] valueColumnSerializers =
			new TypeSerializer<?>[getNumValueColumns()];

		for (int index = 0; index < getNumValueColumns(); ++index) {
			valueColumnSerializers[index] =
					getValueColumnDescriptor(index).getSerializer();
		}

		return new RowSerializer(valueColumnSerializers);
	}

	/**
	 * Returns the merger for the values in the state.
	 *
	 * @return The merger for the values in the state.
	 */
	public Merger<Row> getValueMerger() {
		Merger<?>[] valueColumnMergers =
			new Merger<?>[getNumValueColumns()];

		for (int index = 0; index < getNumValueColumns(); ++index) {
			valueColumnMergers[index] =
				getValueColumnDescriptor(index).getMerger();
		}

		return new RowMerger(valueColumnMergers);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InternalStateDescriptor that = (InternalStateDescriptor) o;

		return name.equals(that.name) &&
			partitioner.equals(that.partitioner) &&
			keyColumnDescriptors.equals(that.keyColumnDescriptors) &&
			valueColumnDescriptors.equals(that.valueColumnDescriptors);
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + partitioner.hashCode();
		result = 31 * result + keyColumnDescriptors.hashCode();
		result = 31 * result + valueColumnDescriptors.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "InternalStateDescriptor{" +
			"name=" + name +
			", partitioner=" + partitioner +
			", keyColumnDescriptors=" + keyColumnDescriptors +
			", valueColumnDescriptors=" + valueColumnDescriptors +
			"}";
	}
}

