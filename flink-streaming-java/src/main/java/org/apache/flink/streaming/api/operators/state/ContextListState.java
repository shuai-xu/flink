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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.keyed.KeyedListState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * An implementation of {@link ListState} which is backed by a
 * {@link KeyedListState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <E> The type of the elements in the state.
 */
public class ContextListState<E> implements ListState<E> {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedListState<Object, E> keyedState;

	public ContextListState(
		final AbstractStreamOperator<?> operator,
		final KeyedListState<Object, E> keyedState
	) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(keyedState);

		this.operator = operator;
		this.keyedState = keyedState;
	}

	@Override
	public Iterable<E> get() throws Exception {
		final List<E> value = keyedState.get(operator.getCurrentKey());
		final List<E> ret = value == null ? Collections.emptyList() : value;
		return () -> ret.iterator();
	}

	@Override
	public void add(E value) throws Exception {
		keyedState.add(operator.getCurrentKey(), value);
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}

	@Override
	public void update(List<E> values) throws Exception {
		keyedState.remove(operator.getCurrentKey());

		if (values.isEmpty()) {
			return;
		}
		keyedState.addAll(operator.getCurrentKey(), values);
	}

	@Override
	public void addAll(List<E> values) throws Exception {
		keyedState.addAll(operator.getCurrentKey(), values);
	}
}

