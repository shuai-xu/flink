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

package com.alibaba.blink.cache;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * LRU Cache support TTL, it can be safely accessed by multiple concurrent threads.
 *
 * @param <K> key
 */
public class LRUCache<K, V> implements Cache<K, V> {

	private final org.apache.flink.shaded.guava18.com.google.common.cache.Cache<K, V> cache;

	public LRUCache(final long cacheSize, boolean isRecordingStats) {

		CacheBuilder builder = CacheBuilder.newBuilder().maximumSize(cacheSize);
		if (isRecordingStats) {
			builder = builder.recordStats();
		}
		this.cache = builder.build();
	}

	public LRUCache(final long cacheSize, final long ttlMs, boolean isRecordingStats) {
		CacheBuilder builder = CacheBuilder
				.newBuilder()
				.maximumSize(cacheSize)
				.expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS);
		if (isRecordingStats) {
			builder = builder.recordStats();
		}
		this.cache = builder.build();
	}

	public V get(K key) {
		return this.cache.getIfPresent(key);
	}

	public void put(K key, V row) {
		this.cache.put(key, row);
	}

	public void remove(K key) {
		this.cache.invalidate(key);
	}

	public long size() {
		return this.cache.size();
	}

	public double hitRate() {
		return this.cache.stats().hitRate();
	}
}
