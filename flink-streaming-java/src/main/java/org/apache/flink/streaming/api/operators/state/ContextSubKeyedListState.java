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
import org.apache.flink.runtime.state.subkeyed.SubKeyedListState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import java.util.Collection;
import java.util.List;

/**
 * used for ListState.
 * @param <N>
 * @param <E>
 */
public class ContextSubKeyedListState<N, E>
	implements ContextSubKeyedAppendingState<N, E, Iterable<E>>, ContextMergingState<N>, ListState<E> {

	private N namespace;

	private final AbstractStreamOperator<?> operator;

	private final SubKeyedListState<Object, N, E> subKeyedListState;

	public ContextSubKeyedListState(AbstractStreamOperator<?> operator, SubKeyedListState<Object, N, E> subKeyedListState) {
		this.operator = operator;
		this.subKeyedListState = subKeyedListState;
	}

	@Override
	public Iterable<E> get() throws Exception {
		return subKeyedListState.get(getCurrentKey(), getNamespace());
	}

	@Override
	public void add(E value) throws Exception {
		subKeyedListState.add(getCurrentKey(), getNamespace(), value);
	}

	@Override
	public void clear() {
		subKeyedListState.remove(getCurrentKey(), getNamespace());
	}

	@Override
	public Object getCurrentKey() {
		return operator.getCurrentKey();
	}

	@Override
	public N getNamespace() {
		return this.namespace;
	}

	@Override
	public void setNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources != null) {
			for (N source : sources) {
				List<E> list = subKeyedListState.get(getCurrentKey(), source);
				if (list != null) {
					subKeyedListState.addAll(getCurrentKey(), target, list);
					subKeyedListState.remove(getCurrentKey(), source);
				}
			}
		}
	}

	@Override
	public void update(List<E> values) throws Exception {

	}

	@Override
	public void addAll(List<E> values) throws Exception {

	}
}
