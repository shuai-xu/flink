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

package com.alibaba.blink.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A cache caches all data in a hashmap. Note this is not a thread safe cache, which needs
 * to be read/write with a read/write lock.
 */
public class AllCache<K, V> implements Cache<K, V> {

	public final ReadWriteLock lock = new ReentrantReadWriteLock();

	public AtomicBoolean isRegisteredTimer = new AtomicBoolean(false);

	private Map<K, V> cache = new HashMap<>(256);
	private Map<K, V> tempCache = null;

	private ScheduledFuture<?> scheduledFuture;

	private volatile long lastUpdated = 0;
	private volatile Exception exception = null;

	public void setException(Exception exception) {
		this.exception = exception;
	}

	/**
	 * Returns whether is loaded or throws exception if errors happen.
	 */
	public boolean isLoadedOrThrowException() throws Exception {
		if (exception != null) {
			throw exception;
		}
		return lastUpdated > 0;
	}

	public boolean isLoaded() {
		return lastUpdated > 0;
	}

	public void initialize() {
		this.tempCache = new HashMap<>(cache.size());
	}

	public void switchCache() {
		lock.writeLock().lock();
		try {
			this.cache = this.tempCache;
		} finally {
			lock.writeLock().unlock();
		}

		this.tempCache = null;
		this.lastUpdated = System.currentTimeMillis();
	}

	public ScheduledFuture<?> getScheduledFuture() {
		return scheduledFuture;
	}

	public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
		this.scheduledFuture = scheduledFuture;
	}

	@Override
	public V get(K key) {
		return cache.get(key);
	}

	@Override
	public void put(K key, V value) {
		tempCache.put(key, value);
	}

	@Override
	public void remove(K key) {
		throw new UnsupportedOperationException("ALL cache do not support remove operation.");
	}

	@Override
	public long size() {
		lock.readLock().lock();
		try {
			return cache.size();
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public double hitRate() {
		throw new UnsupportedOperationException("ALL cache do not support remove operation.");
	}
}
