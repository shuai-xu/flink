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
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * used for folding state.
 * @param <N>
 * @param <T>
 * @param <ACC>
 */
public class ContextSubKeyedFoldingState<N, T, ACC> implements ContextSubKeyedAppendingState<N, T, ACC>, FoldingState<T, ACC> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedValueState<Object, N, ACC> subKeyedValueState;

	private final FoldingStateDescriptor<T, ACC> stateDescriptor;

	private final FoldTransformation foldTransformation;

	public ContextSubKeyedFoldingState(
		AbstractStreamOperator<?> operator,
		SubKeyedValueState<Object, N, ACC> subKeyedValueState,
		FoldingStateDescriptor<T, ACC> stateDescriptor) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(stateDescriptor);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.stateDescriptor = stateDescriptor;
		this.foldTransformation = new FoldTransformation(stateDescriptor.getFoldFunction());
	}

	@Override
	public ACC get() {
		return subKeyedValueState.get(getCurrentKey(), getNamespace());
	}

	@Override
	public void add(T value) {
		subKeyedValueState.transform(operator.getCurrentKey(), getNamespace(), value, foldTransformation);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(getCurrentKey(), getNamespace());
	}

	@Override
	public Object getCurrentKey() {
		return operator.getCurrentKey();
	}

	@Override
	public N getNamespace() {
		return namespace;
	}

	@Override
	public void setNamespace(N namespace) {
		this.namespace = namespace;
	}

	private ACC getInitialValue() {
		return stateDescriptor.getInitialValue();
	}

	private final class FoldTransformation implements StateTransformationFunction<ACC, T> {

		private final FoldFunction<T, ACC> foldFunction;

		FoldTransformation(FoldFunction<T, ACC> foldFunction) {
			this.foldFunction = Preconditions.checkNotNull(foldFunction);
		}

		@Override
		public ACC apply(ACC previousState, T value) throws Exception {
			return foldFunction.fold((previousState != null) ? previousState : getInitialValue(), value);
		}
	}
}
