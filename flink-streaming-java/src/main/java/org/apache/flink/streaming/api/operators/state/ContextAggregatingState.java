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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

/**
 * An implementation of {@link AggregatingState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the operator. That is, when the current key of the operator changes, the
 * values accessed will be changed as well.
 *
 * @param <IN> The type of the values aggregated by the state.
 * @param <ACC> The type of the values stored in the state.
 * @param <OUT> The type of the values returned by the state.
 */

public class ContextAggregatingState<IN, ACC, OUT> implements AggregatingState<IN, OUT> {

	/** The operator to which the state belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, ACC> keyedState;

	/** The descriptor of the state. */
	private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

	public ContextAggregatingState(
		final AbstractStreamOperator<?> operator,
		final KeyedValueState<Object, ACC> keyedState,
		final AggregateFunction<IN, ACC, OUT> aggregateFunction
	) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(aggregateFunction);

		this.operator = operator;
		this.keyedState = keyedState;
		this.aggregateFunction = aggregateFunction;
	}

	@Override
	public OUT get() throws Exception {
		ACC accumulator = keyedState.get(operator.getCurrentKey());
		return accumulator == null ? null : aggregateFunction.getResult(accumulator);
	}

	@Override
	public void add(IN value) throws Exception {
		ACC accumulator = keyedState.get(operator.getCurrentKey());
		if (accumulator == null) {
			accumulator = aggregateFunction.createAccumulator();
		}

		aggregateFunction.add(value, accumulator);

		keyedState.put(operator.getCurrentKey(), accumulator);
	}

	@Override
	public void clear() {
		keyedState.remove(operator.getCurrentKey());
	}
}
