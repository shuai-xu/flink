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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.keyed.ContextKeyedState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * An implementation of {@link ValueState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <T> The type of the values in the state.
 */
public class ContextValueState<T> implements ValueState<T>, ContextKeyedState {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, T> keyedState;

	/** The descriptor of the state. */
	private final ValueStateDescriptor<T> stateDescriptor;

	public ContextValueState(
		final AbstractStreamOperator<?> operator,
		final KeyedValueState<Object, T> keyedState,
		final ValueStateDescriptor<T> stateDescriptor
	) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(stateDescriptor);

		this.operator = operator;
		this.keyedState = keyedState;
		this.stateDescriptor = stateDescriptor;
	}

	@Override
	public T value() throws IOException {
		Object key = operator.getCurrentKey();
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");

		T value = keyedState.get(key);
		return value == null ? stateDescriptor.getDefaultValue() : value;
	}

	@Override
	public void update(T value) throws IOException {
		keyedState.put(operator.getCurrentKey(), value);
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}

	@Override
	public KeyedState getKeyedState() {
		return keyedState;
	}
}
