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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The descriptor for both local and global {@link KeyedState}.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the values in the state.
 * @param <S> The type of the state described by the descriptor
 */
public abstract class KeyedStateDescriptor<K, V, S extends KeyedState<K, V>> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The name of the state.
	 */
	private final String name;

	/**
	 * The serializer for the keys in the state.
	 */
	private final TypeSerializer<K> keySerializer;

	/**
	 * The serializer for the values in the state.
	 */
	private final TypeSerializer<V> valueSerializer;

	/**
	 * The merger for the values in the state. This is null in the cases where
	 * the described state is global.
	 */
	@Nullable
	private final Merger<V> valueMerger;

	/**
	 * Constructor for global states with given name and the serializers for
	 * the keys and the values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 */
	KeyedStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<V> valueSerializer
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(valueSerializer);

		this.name = name;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.valueMerger = null;
	}

	/**
	 * Constructor for with given name, scope and the serializers for the keys
	 * and the values in the state.
	 *
	 * @param name The name of the state.
	 * @param keySerializer The serializer for the keys in the state.
	 * @param valueSerializer The serializer for the values in the state.
	 * @param valueMerger The merger for the values in the state.
	 */
	KeyedStateDescriptor(
		final String name,
		final TypeSerializer<K> keySerializer,
		final TypeSerializer<V> valueSerializer,
		final Merger<V> valueMerger
	) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(keySerializer);
		Preconditions.checkNotNull(valueSerializer);

		this.name = name;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.valueMerger = valueMerger;
	}

	/**
	 * Creates the state described by the descriptor with the given binder.
	 *
	 * @param stateBinder The binder with which to create the state.
	 * @return The state described by the descriptor.
	 */
	public abstract S bind(KeyedStateBinder stateBinder);

	//--------------------------------------------------------------------------

	/**
	 * Returns the name of the state.
	 *
	 * @return The name of the state.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the serializer for the keys in the state.
	 *
	 * @return The serializer for the keys in the state.
	 */
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Returns the serializer for the values in the state.
	 *
	 * @return The serializer for the values in the state.
	 */
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Returns the merger for the values in the state.
	 *
	 * @return The merger for the values in the state.
	 */
	public Merger<V> getValueMerger() {
		return valueMerger;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KeyedStateDescriptor<?, ?, ?> that = (KeyedStateDescriptor<?, ?, ?>) o;

		return Objects.equals(name, that.name) &&
			Objects.equals(keySerializer, that.keySerializer) &&
			Objects.equals(valueSerializer, that.valueSerializer) &&
			Objects.equals(valueMerger, that.valueMerger);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(name);
		result = 31 * result + Objects.hashCode(keySerializer);
		result = 31 * result + Objects.hashCode(valueSerializer);
		result = 31 * result + Objects.hashCode(valueMerger);
		return result;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "{" +
			"name=" + name +
			", keySerializer=" + keySerializer +
			", valueSerializer=" + valueSerializer +
			", valueMerger=" + valueMerger +
			"}";
	}
}
