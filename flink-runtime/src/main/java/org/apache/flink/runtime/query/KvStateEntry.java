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

package org.apache.flink.runtime.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An entry holding the {@link InternalKvState} along with its {@link KvStateInfo}.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace the state is associated to
 * @param <V> The type of values kept internally in state
 */
@Internal
public class KvStateEntry<K, N, V> {

	private final KeyedState<K, V> state;

	private final ConcurrentMap<Thread, KvStateInfo<K, N, V>> serializerCache;

	public KvStateEntry(final KeyedState<K, V> state) {
		this.state = Preconditions.checkNotNull(state);

		this.serializerCache = new ConcurrentHashMap<>();
	}

	public KeyedState<K, V> getState() {
		return state;
	}

	public void clear() {
		serializerCache.clear();
	}

	@VisibleForTesting
	public int getCacheSize() {
		return serializerCache.size();
	}
}
