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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.runtime.state.keyed.ContextKeyedState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * An implementation of {@link FoldingState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <T> The type of the values in the state.
 */
public class ContextFoldingState<T, ACC> implements FoldingState<T, ACC>, ContextKeyedState {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, ACC> keyedState;

	/** The descriptor of the state. */
	private final FoldingStateDescriptor<T, ACC> stateDescriptor;

	public ContextFoldingState(
		final AbstractStreamOperator<?> operator,
		final KeyedValueState<Object, ACC> keyedState,
		final FoldingStateDescriptor<T, ACC> stateDescriptor
	) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(stateDescriptor);

		this.operator = operator;
		this.keyedState = keyedState;
		this.stateDescriptor = stateDescriptor;
	}

	@Override
	public ACC get() throws Exception {
		return keyedState.get(operator.getCurrentKey());
	}

	@Override
	public void add(T value) throws Exception {
		FoldFunction<T, ACC> foldFunction = stateDescriptor.getFoldFunction();

		ACC oldValue = keyedState.get(operator.getCurrentKey());
		ACC newValue = oldValue == null ?
			foldFunction.fold(getInitialValue(), value) :
			foldFunction.fold(oldValue, value);
		keyedState.put(operator.getCurrentKey(), newValue);
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}

	private ACC getInitialValue() {
		return stateDescriptor.getInitialValue();
	}

	@Override
	public KeyedState getKeyedState() {
		return keyedState;
	}
}
