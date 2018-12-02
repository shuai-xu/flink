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

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.SortedMapState;
import org.apache.flink.api.common.state.SortedMapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.runtime.state.keyed.KeyedListStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedMapState;
import org.apache.flink.runtime.state.keyed.KeyedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapState;
import org.apache.flink.runtime.state.keyed.KeyedSortedMapStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * A helper class to create user-facing states.
 */
public class ContextStateBinder implements StateBinder {

	/** The operator to create states. */
	private final AbstractStreamOperator<?> operator;

	/** All {@link State}s created by this binder. */
	private final Map<String, State> states;

	public ContextStateBinder(AbstractStreamOperator<?> operator) {
		Preconditions.checkNotNull(operator);
		this.operator = operator;
		this.states = new HashMap<>();
	}

	@Override
	public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedValueStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedValueState<Object, T> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextValueState<>(operator, keyedState, stateDesc);
			states.put(stateName, state);
		}
		return (ValueState) state;
	}

	@Override
	public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedListStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedListStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getElementSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedListState<Object, T> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextListState<>(operator, keyedState);
			states.put(stateName, state);
		}
		return (ListState) state;
	}

	@Override
	public <MK, MV> MapState<MK, MV> createMapState(
		MapStateDescriptor<MK, MV> stateDesc
	) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedMapStateDescriptor<Object, MK, MV> keyedStateDescriptor =
				new KeyedMapStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getKeySerializer(),
					stateDesc.getValueSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedMapState<Object, MK, MV> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextMapState<>(operator, keyedState);
			states.put(stateName, state);
		}
		return (MapState) state;
	}

	@Override
	public <MK, MV> SortedMapState<MK, MV> createSortedMapState(SortedMapStateDescriptor<MK, MV> stateDesc) {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedSortedMapStateDescriptor<Object, MK, MV> keyedStateDescriptor =
				new KeyedSortedMapStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedSortedMapState<Object, MK, MV> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextSortedMapState<>(operator, keyedState);
			states.put(stateName, state);
		}
		return (SortedMapState) state;
	}

	@Override
	public <T> ReducingState<T> createReducingState(
		ReducingStateDescriptor<T> stateDesc
	) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedValueStateDescriptor<Object, T> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedValueState<Object, T> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextReducingState<>(operator, keyedState, stateDesc.getReduceFunction());
			states.put(stateName, state);
		}
		return (ReducingState) state;
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> createFoldingState(
		FoldingStateDescriptor<T, ACC> stateDesc
	) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedValueStateDescriptor<Object, ACC> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getSerializer()
				);
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedValueState<Object, ACC> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextFoldingState<>(operator, keyedState, stateDesc);
			states.put(stateName, state);
		}
		return (FoldingState) state;
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> createAggregatingState(
		AggregatingStateDescriptor<IN, ACC, OUT> stateDesc
	) throws Exception {
		Preconditions.checkNotNull(stateDesc);

		String stateName = stateDesc.getName();

		State state = states.get(stateName);

		if (state == null) {
			stateDesc.initializeSerializerUnlessSet(operator.getExecutionConfig());

			KeyedValueStateDescriptor<Object, ACC> keyedStateDescriptor =
				new KeyedValueStateDescriptor<>(
					stateDesc.getName(),
					operator.getKeySerializer(),
					stateDesc.getSerializer());
			if (stateDesc.isQueryable()) {
				keyedStateDescriptor.setQueryable(stateDesc.getQueryableStateName());
			}

			KeyedValueState<Object, ACC> keyedState = operator.getKeyedState(keyedStateDescriptor);

			state = new ContextAggregatingState<>(operator, keyedState, stateDesc.getAggregateFunction());
			states.put(stateName, state);
		}
		return (AggregatingState) state;
	}
}
