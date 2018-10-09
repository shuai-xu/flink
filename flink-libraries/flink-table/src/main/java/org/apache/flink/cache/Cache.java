/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cache;


/**
 * A semi-persistent mapping from keys to values. Cache entries are manually added using
 * {@link #put(Object, Object)}, and are stored in the cache until either evicted or
 * manually invalidated.
 *
 * <p>Implementations of this interface are expected to be thread-safe, and can be safely accessed
 * by multiple concurrent threads.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Cache<K, V> {

	/**
	 * Returns the value associated with {@code key} in this cache, or {@code null} if there is no
	 * cached value for {@code key}.
	 */
	V get(K key);

	/**
	 * Associates {@code value} with {@code key} in this cache. If the cache previously contained a
	 * value associated with {@code key}, the old value is replaced by {@code value}.
	 */
	void put(K key, V value);

	/**
	 * Discards any cached value for key {@code key}.
	 */
	void remove(K key);

	/**
	 * Returns the approximate number of entries in this cache.
	 */
	long size();

	/**
	 * Returns the ratio of cache requests which were hits. This is defined as
	 * {@code hitCount / requestCount}, or {@code 1.0} when {@code requestCount == 0}.
	 * Note that {@code hitRate + missRate =~ 1.0}.
	 */
	double hitRate();
}
