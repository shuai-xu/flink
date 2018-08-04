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

package org.apache.flink.runtime.state.subkeyed;

/**
 * The interface for {@link SubKeyedState} whose values are single-values.
 *
 * @param <K> The type of the keys in the state.
 * @param <N> The type of the namespaces in the state.
 * @param <V> The type of the keys in the state.
 */
public interface SubKeyedValueState<K, N, V> extends SubKeyedState<K, N, V> {

	/**
	 * Associates the given value with the given key and namespace in the state.
	 * If there already exists a value under the given key and namespace, the
	 * value will be replaced with the given value.
	 *
	 * @param key The key under which the given value is to be associated.
	 * @param namespace The namespace of the value to be associated.
	 * @param value The value to be associated.
	 */
	void put(K key, N namespace, V value);
}
