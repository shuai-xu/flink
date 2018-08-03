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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * A helper class to construct {@link InternalStateDescriptor}.
 */
public final class InternalStateDescriptorBuilder {

	/**
	 * The name of the state.
	 */
	private final String name;

	/**
	 * The partitioner for the keys in the state. By default, the state is hash
	 * partitioned.
	 */
	private Partitioner<Row> partitioner;

	/**
	 * The descriptors for the key columns in the state.
	 */
	private List<InternalColumnDescriptor<?>> keyColumnDescriptors;

	/**
	 * The descriptors for the value columns in the state.
	 */
	private List<InternalColumnDescriptor<?>> valueColumnDescriptors;

	//--------------------------------------------------------------------------

	/**
	 * Construct the builder of a state's descriptor with the state's name.
	 *
	 * @param name The name of the state.
	 */
	public InternalStateDescriptorBuilder(String name) {
		this.name = name;
		this.keyColumnDescriptors = new ArrayList<>();
		this.valueColumnDescriptors = new ArrayList<>();
		this.partitioner = new HashPartitioner<>();
	}

	/**
	 * Sets the partition method in the state's descriptor.
	 *
	 * @param partitioner The partitioner for the keys in the state.
	 * @return The builder for the state's descriptor.
	 */
	public InternalStateDescriptorBuilder setPartitioner(Partitioner<Row> partitioner) {
		this.partitioner = partitioner;
		return this;
	}

	/**
	 * Adds an unordered key column into the state's descriptor.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param <K> Type of the objects in the column.
	 * @return The builder for this state's descriptor.
	 */
	public <K> InternalStateDescriptorBuilder addKeyColumn(
		String name,
		TypeSerializer<K> serializer
	) {
		InternalColumnDescriptor<K> keyColumnDescriptor =
			new InternalColumnDescriptor<>(name, serializer);
		keyColumnDescriptors.add(keyColumnDescriptor);

		return this;
	}

	/**
	 * Adds an ordered key column into the state's descriptor.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param comparator The comparator for the objects in the column.
	 * @param <K> Type of the objects in the column.
	 * @return The builder for this state's descriptor.
	 */
	public <K> InternalStateDescriptorBuilder addKeyColumn(
		String name,
		TypeSerializer<K> serializer,
		Comparator<K> comparator
	) {
		InternalColumnDescriptor<K> keyColumnDescriptor =
			comparator == null ?
				new InternalColumnDescriptor<>(name, serializer) :
				new InternalColumnDescriptor<>(name, serializer, comparator);
		keyColumnDescriptors.add(keyColumnDescriptor);

		return this;
	}

	/**
	 * Adds a value column into the state's descriptor.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param <V> Type of the objects in the column.
	 * @return The builder for this state's descriptor.
	 */
	public <V> InternalStateDescriptorBuilder addValueColumn(
		String name,
		TypeSerializer<V> serializer
	) {
		InternalColumnDescriptor<V> valueColumnDescriptor =
			new InternalColumnDescriptor<>(name, serializer);
		valueColumnDescriptors.add(valueColumnDescriptor);

		return this;
	}

	/**
	 * Adds a value column into the state's descriptor.
	 *
	 * @param name The name of the column.
	 * @param serializer The serializer for the objects in the column.
	 * @param merger The merger for the objects in the column.
	 * @param <V> Type of the objects in the column.
	 * @return The builder for this state's descriptor.
	 */
	public <V> InternalStateDescriptorBuilder addValueColumn(
		String name,
		TypeSerializer<V> serializer,
		Merger<V> merger
	) {
		InternalColumnDescriptor<V> valueColumnDescriptor =
			merger == null ?
				new InternalColumnDescriptor<>(name, serializer) :
				new InternalColumnDescriptor<>(name, serializer, merger);
		valueColumnDescriptors.add(valueColumnDescriptor);

		return this;
	}

	/**
	 * Constructs the descriptor for the state.
	 *
	 * @return The descriptor for the state.
	 */
	public InternalStateDescriptor getDescriptor() {
		return new InternalStateDescriptor(
			name,
			partitioner,
			keyColumnDescriptors,
			valueColumnDescriptors
		);
	}

}

