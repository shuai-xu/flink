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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * used for reducing state.
 * @param <N>
 * @param <T>
 */
public class ContextSubKeyedReducingState<N, T>
	implements ContextSubKeyedAppendingState<N, T, T>, ContextMergingState<N>, ReducingState<T> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedValueState<Object, N, T> subKeyedValueState;

	private final ReduceTransformation transformation;

	public ContextSubKeyedReducingState(
		AbstractStreamOperator<?> operator,
		SubKeyedValueState<Object, N, T> subKeyedValueState,
		ReduceFunction<T> reduceFunction) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(reduceFunction);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.transformation = new ReduceTransformation(reduceFunction);
	}

	@Override
	public T get() {
		return subKeyedValueState.get(getCurrentKey(), getNamespace());
	}

	@Override
	public void add(T value) {
		subKeyedValueState.transform(operator.getCurrentKey(), getNamespace(), value, transformation);
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

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		Object currentKey = getCurrentKey();
		T merged = null;

		// merge the sources
		for (N source : sources) {

			// get and remove the next source per namespace/key
			T sourceState = subKeyedValueState.getAndRemove(currentKey, source);

			if (merged != null && sourceState != null) {
				merged = transformation.reduceFunction.reduce(merged, sourceState);
			} else if (merged == null) {
				merged = sourceState;
			}
		}

		// merge into the target, if needed
		if (merged != null) {
			subKeyedValueState.transform(currentKey, target, merged, transformation);
		}
	}

	private class ReduceTransformation implements StateTransformationFunction<T, T> {

		private final ReduceFunction<T> reduceFunction;

		public ReduceTransformation(ReduceFunction<T> reduceFunction) {
			this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
		}

		@Override
		public T apply(T previousState, T value) throws Exception {
			return previousState == null ? value : reduceFunction.reduce(previousState, value);
		}
	}
}
