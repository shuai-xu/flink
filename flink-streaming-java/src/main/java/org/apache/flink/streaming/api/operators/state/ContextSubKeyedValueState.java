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
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A helper class to get SubKeyedValueState.
 * @param <V>
 * @param <N>
 */
public class ContextSubKeyedValueState<V, N> implements ValueState<V>, ContextSubKeyedState<N> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedValueState<Object, N, V> subKeyedValueState;

	private final V defaultValue;

	public ContextSubKeyedValueState(
		AbstractStreamOperator<?> operator,
		SubKeyedValueState<Object, N, V> subKeyedValueState,
		V defaultValue) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.defaultValue = defaultValue;
	}

	@Override
	public V value() throws IOException {
		V value = subKeyedValueState.get(getCurrentKey(), getNamespace());
		return value == null ? defaultValue : value;
	}

	@Override
	public void update(V value) throws IOException {
		subKeyedValueState.put(getCurrentKey(), getNamespace(), value);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(getCurrentKey(), getNamespace());
	}

	@Override
	public Object getCurrentKey() {
		return this.operator.getCurrentKey();
	}

	@Override
	public N getNamespace() {
		return namespace;
	}

	@Override
	public void setNamespace(N namespace) {
		this.namespace = namespace;
	}
}
