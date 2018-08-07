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
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * used for aggregating state.
 * @param <N>
 * @param <IN>
 * @param <ACC>
 * @param <OUT>
 */
public class ContextSubKeyedAggregatingState<N, IN, ACC, OUT>
	implements ContextSubKeyedAppendingState<N, IN, OUT>, ContextMergingState<N>, AggregatingState<IN, OUT> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedValueState<Object, N, ACC> subKeyedValueState;

	private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

	public ContextSubKeyedAggregatingState(
		AbstractStreamOperator<?> operator,
		SubKeyedValueState<Object, N, ACC> subKeyedValueState,
		AggregateFunction<IN, ACC, OUT> aggregateFunction) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(aggregateFunction);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.aggregateFunction = aggregateFunction;
	}

	@Override
	public OUT get() throws Exception {
		ACC accumulator = subKeyedValueState.get(getCurrentKey(), getNamespace());
		return accumulator == null ? null : aggregateFunction.getResult(accumulator);
	}

	@Override
	public void add(IN value) throws Exception {
		ACC accumulator = subKeyedValueState.get(getCurrentKey(), getNamespace());
		if (accumulator == null) {
			accumulator = aggregateFunction.createAccumulator();
		}
		aggregateFunction.add(value, accumulator);
		subKeyedValueState.put(getCurrentKey(), getNamespace(), accumulator);
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
		if (sources != null) {
			ACC currACC = subKeyedValueState.get(getCurrentKey(), target);
			if (currACC == null) {
				currACC = aggregateFunction.createAccumulator();
			}
			for (N source : sources) {
				ACC fromACC = subKeyedValueState.get(getCurrentKey(), source);
				if (fromACC == null) {
					fromACC = aggregateFunction.createAccumulator();
				}
				currACC = aggregateFunction.merge(currACC, fromACC);
				subKeyedValueState.remove(getCurrentKey(), source);
			}
			subKeyedValueState.put(getCurrentKey(), target, currACC);
		}
	}
}
