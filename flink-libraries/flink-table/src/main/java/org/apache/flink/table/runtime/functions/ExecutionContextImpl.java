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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state2.ListState;
import org.apache.flink.api.common.state2.ListStateDescriptor;
import org.apache.flink.api.common.state2.MapState;
import org.apache.flink.api.common.state2.MapStateDescriptor;
import org.apache.flink.api.common.state2.SortedMapState;
import org.apache.flink.api.common.state2.SortedMapStateDescriptor;
import org.apache.flink.api.common.state2.ValueState;
import org.apache.flink.api.common.state2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.runtime.state2.keyed.KeyedListState;
import org.apache.flink.runtime.state2.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state2.keyed.KeyedMapState;
import org.apache.flink.runtime.state2.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state2.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state2.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state2.keyed.KeyedState;
import org.apache.flink.runtime.state2.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state2.keyed.KeyedValueState;
import org.apache.flink.runtime.state2.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state2.partitioned.PartitionedListStateDescriptor;
import org.apache.flink.runtime.state2.partitioned.PartitionedMapStateDescriptor;
import org.apache.flink.runtime.state2.partitioned.PartitionedSortedMapStateDescriptor;
import org.apache.flink.runtime.state2.partitioned.PartitionedValueStateDescriptor;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedSortedMapState;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state2.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;


/**
 * Implementation of ExecutionContext.
 */
@SuppressWarnings("unchecked")
public final class ExecutionContextImpl implements ExecutionContext {

	private final AbstractStreamOperator<?> operator;
	private final RuntimeContext runtimeContext;
	private final TypeSerializer<?> namespaceSerializer;

	public ExecutionContextImpl(
			AbstractStreamOperator<?> operator,
			RuntimeContext runtimeContext) {
		this(operator, runtimeContext, null);
	}

	public ExecutionContextImpl(
			AbstractStreamOperator<?> operator,
			RuntimeContext runtimeContext,
			TypeSerializer<?> namespaceSerializer) {
		this.operator = Preconditions.checkNotNull(operator);
		this.runtimeContext = Preconditions.checkNotNull(runtimeContext);
		this.namespaceSerializer = namespaceSerializer;
	}

	@Override
	public <K, V, S extends KeyedState<K, V>> S getKeyedState(KeyedStateDescriptor<K, V, S> descriptor) {
		return operator.getKeyedState(descriptor);
	}

	@Override
	public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(SubKeyedStateDescriptor<K, N, V, S> descriptor) {
		return operator.getSubKeyedState(descriptor);
	}

	@Override
	public <K, V> KeyedValueState<K, V> getKeyedValueState(
		ValueStateDescriptor<V> descriptor
	) {
		return operator.getKeyedState(
			new KeyedValueStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				descriptor.getSerializer(operator.getExecutionConfig())
			)
		);
	}

	@Override
	public <K, V> KeyedListState<K, V> getKeyedListState(
		ListStateDescriptor<V> descriptor
	) {
		return operator.getKeyedState(
			new KeyedListStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				descriptor.getSerializer(operator.getExecutionConfig())
			)
		);
	}

	@Override
	public <K, UK, UV> KeyedMapState<K, UK, UV> getKeyedMapState(
		MapStateDescriptor<UK, UV> descriptor
	) {
		return operator.getKeyedState(
			new KeyedMapStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				descriptor.getSerializer(operator.getExecutionConfig())
			)
		);
	}

	@Override
	public <K, UK, UV> KeyedSortedMapState<K, UK, UV> getKeyedSortedMapState(
		SortedMapStateDescriptor<UK, UV> descriptor
	) {
		return operator.getKeyedState(
			new KeyedSortedMapStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				descriptor.getSerializer(operator.getExecutionConfig())
			)
		);
	}

	@Override
	public <K, N, V> SubKeyedValueState<K, N, V> getSubKeyedValueState(
		ValueStateDescriptor<V> descriptor
	) {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		return operator.getSubKeyedState(
			new SubKeyedValueStateDescriptor<>(
				descriptor.getName(),
				(TypeSerializer<K>) operator.getKeySerializer(),
				(TypeSerializer<N>) namespaceSerializer,
				descriptor.getSerializer(operator.getExecutionConfig())
			)
		);
	}

	@Override
	public <K, N, V> SubKeyedListState<K, N, V> getSubKeyedListState(
		ListStateDescriptor<V> descriptor
	) {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		return operator.getSubKeyedState(new SubKeyedListStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			descriptor.getSerializer(operator.getExecutionConfig()).getElementSerializer()));
	}

	@Override
	public <K, N, UK, UV> SubKeyedMapState<K, N, UK, UV> getSubKeyedMapState(
		MapStateDescriptor<UK, UV> descriptor
	) {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		MapSerializer<UK, UV> mapSerializer = descriptor.getSerializer(operator.getExecutionConfig());
		return operator.getSubKeyedState(new SubKeyedMapStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			mapSerializer.getKeySerializer(),
			mapSerializer.getValueSerializer()));
	}

	@Override
	public <K, N, UK, UV> SubKeyedSortedMapState<K, N, UK, UV> getSubKeyedSortedMapState(
		SortedMapStateDescriptor<UK, UV> descriptor
	) {
		if (namespaceSerializer == null) {
			throw new RuntimeException("The namespace serializer has not been initialized.");
		}

		SortedMapSerializer<UK, UV> sortedMapSerializer = descriptor.getSerializer(operator.getExecutionConfig());
		return operator.getSubKeyedState(new SubKeyedSortedMapStateDescriptor<>(
			descriptor.getName(),
			(TypeSerializer<K>) operator.getKeySerializer(),
			(TypeSerializer<N>) namespaceSerializer,
			sortedMapSerializer.getComparator(),
			sortedMapSerializer.getKeySerializer(),
			sortedMapSerializer.getValueSerializer()));
	}

	@Override
	public <V> ValueState<V> getPartitionedValueState(PartitionedValueStateDescriptor<V> descriptor) {
		return operator.getPartitionedState(descriptor);
	}

	@Override
	public <V> ListState<V> getPartitionedListState(PartitionedListStateDescriptor<V> descriptor) {
		return operator.getPartitionedState(descriptor);
	}

	@Override
	public <UK, UV> MapState<UK, UV> getPartitionedMapState(PartitionedMapStateDescriptor<UK, UV> descriptor) {
		return operator.getPartitionedState(descriptor);
	}

	@Override
	public <UK, UV> SortedMapState<UK, UV> getPartitionedSortedMapState(
		PartitionedSortedMapStateDescriptor<UK, UV> descriptor) {
		return operator.getPartitionedState(descriptor);
	}

	@Override
	public <K> TypeSerializer<K> getKeySerializer() {
		return (TypeSerializer<K>) operator.getKeySerializer();
	}

	@Override
	public BaseRow currentKey() {
		return (BaseRow) operator.getCurrentKey();
	}

	@Override
	public void setCurrentKey(BaseRow key) {
		operator.setCurrentKey(key);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return runtimeContext;
	}
}
