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

package org.apache.flink.streaming.api.operators.state;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedListStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * A helper class to create sub keyed states.
 */
public class ContextSubKeyedStateBinder {

	/** The operator to create states. */
	private final AbstractStreamOperator<?> operator;

	public ContextSubKeyedStateBinder(AbstractStreamOperator<?> operator) {
		Preconditions.checkNotNull(operator);
		this.operator = operator;
	}

	private <S extends State, T, N> S createValueState(
		ValueStateDescriptor<T> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedValueStateDescriptor<Object, N, T> subKeyedValueStateDescriptor =
			new SubKeyedValueStateDescriptor<>(
				stateDesc.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDesc.getSerializer()
			);

		SubKeyedValueState subKeyedValueState = operator.getSubKeyedState(subKeyedValueStateDescriptor);
		ContextSubKeyedValueState state =  new ContextSubKeyedValueState<T, N>(this.operator, subKeyedValueState, stateDesc.getDefaultValue());
		state.setNamespace(namespace);
		return (S) state;
	}

	private <S extends State, T, N> S createListState(
		ListStateDescriptor<T> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedListStateDescriptor<Object, N, T> subKeyedListStateDescriptor =
			new SubKeyedListStateDescriptor<>(
				stateDesc.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDesc.getElementSerializer()
			);

		SubKeyedListState subKeyedListState = operator.getSubKeyedState(subKeyedListStateDescriptor);
		ContextSubKeyedListState<N, T> state = new ContextSubKeyedListState<N, T>(
			this.operator,
			subKeyedListState
		);
		state.setNamespace(namespace);

		return (S) state;
	}

	private <S extends State, T, N> S createReducingState(
		ReducingStateDescriptor<T> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedValueStateDescriptor<Object, N, T> subKeyedValueStateDescriptor =
			new SubKeyedValueStateDescriptor<>(
				stateDesc.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDesc.getSerializer()
			);

		SubKeyedValueState subKeyedValueState = operator.getSubKeyedState(subKeyedValueStateDescriptor);
		ContextSubKeyedReducingState<N, T> state = new ContextSubKeyedReducingState<N, T>(
			this.operator,
			subKeyedValueState,
			stateDesc.getReduceFunction()
		);
		state.setNamespace(namespace);

		return (S) state;
	}

	private <S extends State, N, IN, ACC, OUT> S createAggregatingState(
		AggregatingStateDescriptor<IN, ACC, OUT> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedValueStateDescriptor<Object, N, ACC> subKeyedValueStateDescriptor =
			new SubKeyedValueStateDescriptor<>(
				stateDesc.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDesc.getSerializer()
			);
		SubKeyedValueState subKeyedValueState = operator.getSubKeyedState(subKeyedValueStateDescriptor);
		ContextSubKeyedAggregatingState<N, IN, ACC, OUT> state =
			new ContextSubKeyedAggregatingState<>(
				operator,
				subKeyedValueState,
				stateDesc.getAggregateFunction()
			);

		state.setNamespace(namespace);

		return (S) state;
	}

	private <S extends State, N, IN, ACC> S createFoldingState(
		FoldingStateDescriptor<IN, ACC> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedValueStateDescriptor<Object, N, ACC> subKeyedValueStateDescriptor =
			new SubKeyedValueStateDescriptor<>(
				stateDesc.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDesc.getSerializer()
			);

		SubKeyedValueState subKeyedValueState = operator.getSubKeyedState(subKeyedValueStateDescriptor);

		ContextSubKeyedFoldingState<N, IN, ACC> state = new ContextSubKeyedFoldingState<>(
			operator,
			subKeyedValueState,
			stateDesc
		);

		state.setNamespace(namespace);
		return (S) state;
	}

	private <S extends State, N, MK, MV> S createMapState(
		MapStateDescriptor<MK, MV> stateDesc,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception {

		Preconditions.checkNotNull(stateDesc);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(namespaceSerializer);

		stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

		SubKeyedMapStateDescriptor<Object, N, MK, MV> subKeyedMapStateDescriptor = new SubKeyedMapStateDescriptor(
			stateDesc.getName(),
			operator.getKeySerializer(),
			namespaceSerializer,
			stateDesc.getKeySerializer(),
			stateDesc.getValueSerializer()
		);

		SubKeyedMapState<Object, N, MK, MV> subKeyedMapState = operator.getSubKeyedState(subKeyedMapStateDescriptor);

		ContextSubKeyedMapState<N, MK, MV> state = new ContextSubKeyedMapState<>(
			operator,
			subKeyedMapState);

		state.setNamespace(namespace);

		return (S) state;
	}

	public <S extends State, N> S getSubKeyedStateWithNamespace (
		StateDescriptor<S, ?> stateDescriptor,
		N namespace,
		TypeSerializer<N> namespaceSerializer) throws Exception{

		switch (stateDescriptor.getType()) {
			case VALUE:
				return createValueState(
					(ValueStateDescriptor<?>) stateDescriptor, namespace, namespaceSerializer);
			case LIST:
				return createListState(
					(ListStateDescriptor<?>) stateDescriptor, namespace, namespaceSerializer);
			case MAP:
				return createMapState(
					(MapStateDescriptor<?, ?>) stateDescriptor, namespace, namespaceSerializer);
			case FOLDING:
				return createFoldingState(
					(FoldingStateDescriptor<?, ?>) stateDescriptor, namespace, namespaceSerializer);
			case REDUCING:
				return createReducingState(
					(ReducingStateDescriptor<?>) stateDescriptor, namespace, namespaceSerializer);
			case AGGREGATING:
				return createAggregatingState(
					(AggregatingStateDescriptor<?, ?, ?>) stateDescriptor, namespace, namespaceSerializer);
			default:
				throw new RuntimeException("Not a supported State: " + stateDescriptor.getType());
		}
	}

	public <N, V, S extends State> ContextSubKeyedValueState<V, N> getContextSubKeyedValueState(
		StateDescriptor<S, V> stateDescriptor,
		TypeSerializer<N> namespaceSerializer
	) {
		SubKeyedValueStateDescriptor<Object, N, V> subKeyedValueStateDescriptor =
			new SubKeyedValueStateDescriptor<>(
				stateDescriptor.getName(),
				operator.getKeySerializer(),
				namespaceSerializer,
				stateDescriptor.getSerializer()
			);
		return new ContextSubKeyedValueState<>(
			operator,
			operator.getSubKeyedState(subKeyedValueStateDescriptor),
			stateDescriptor.getDefaultValue()
		);
	}

	public <N, IN, ACC, OUT> ContextSubKeyedAppendingState<N, IN, OUT> getContextSubKeyedAppendingState(
		StateDescriptor<? extends AppendingState<?, ?>, ?> stateDescriptor,
		TypeSerializer<N> namespaceSerializer
	) {
		stateDescriptor.initializeSerializerUnlessSet(operator.getExecutionConfig());
		switch (stateDescriptor.getType()) {
			case LIST:
				SubKeyedListStateDescriptor<Object, N, IN> subKeyedListStateDescriptor =
					new SubKeyedListStateDescriptor<Object, N, IN>(
						stateDescriptor.getName(),
						operator.getKeySerializer(),
						namespaceSerializer,
						((ListStateDescriptor<IN>) stateDescriptor).getElementSerializer());
				return (ContextSubKeyedAppendingState<N, IN, OUT>) new ContextSubKeyedListState<N, IN>(
					this.operator,
					this.operator.getSubKeyedState(subKeyedListStateDescriptor));
			case AGGREGATING:
				SubKeyedValueStateDescriptor<Object, N, ACC> aggStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDescriptor.getName(),
						operator.getKeySerializer(),
						namespaceSerializer,
						((AggregatingStateDescriptor<IN, ACC, OUT>) stateDescriptor).getSerializer());
				return new ContextSubKeyedAggregatingState<N, IN, ACC, OUT>(
					operator,
					operator.getSubKeyedState(aggStateDescriptor),
					((AggregatingStateDescriptor<IN, ACC, OUT>) stateDescriptor).getAggregateFunction()
				);
			case FOLDING:
				SubKeyedValueStateDescriptor<Object, N, ACC> foldingStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDescriptor.getName(),
						operator.getKeySerializer(),
						namespaceSerializer,
						((FoldingStateDescriptor<IN, ACC>) stateDescriptor).getSerializer());
				return (ContextSubKeyedAppendingState<N, IN, OUT>) new ContextSubKeyedFoldingState<N, IN, ACC>(
					operator,
					operator.getSubKeyedState(foldingStateDescriptor),
					(FoldingStateDescriptor<IN, ACC>) stateDescriptor);
			case REDUCING:
				SubKeyedValueStateDescriptor<Object, N, IN> reducingStateDescriptor =
					new SubKeyedValueStateDescriptor<>(
						stateDescriptor.getName(),
						operator.getKeySerializer(),
						namespaceSerializer,
						((ReducingStateDescriptor<IN>) stateDescriptor).getSerializer());
				return (ContextSubKeyedAppendingState<N, IN, OUT>) new ContextSubKeyedReducingState<N, IN>(
					operator,
					operator.getSubKeyedState(reducingStateDescriptor),
					((ReducingStateDescriptor<IN>) stateDescriptor).getReduceFunction());
			default:
				throw new RuntimeException("Not a supported AppendingState: " + stateDescriptor.getType());
		}
	}

}
