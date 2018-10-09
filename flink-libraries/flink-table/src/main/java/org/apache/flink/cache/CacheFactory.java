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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating cache.
 */
public class CacheFactory<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(CacheFactory.class);

	private Map<String, Cache<K, V>> cacheMap = new HashMap<>();

	private static CacheFactory instance;

	private CacheFactory() {
	}

	public static synchronized <K, V> CacheFactory<K, V> getInstance() {
		if (instance == null) {
			instance = new CacheFactory<K, V>();
		}
		return instance;
	}

	public synchronized Cache<K, V> getCache(String tableName, CacheStrategy cacheStrategy) {
		if (cacheStrategy.isNoCache()) {                // NONE
			return createNoneCache(tableName);
		} else if (cacheStrategy.isAllCache()) {        // ALL
			return createAllCache(tableName, cacheStrategy.getTtlMs());
		} else if (cacheStrategy.isNeverExpired()) {    // LRU & never expire
			return createLRUCache(tableName, cacheStrategy.getSize(), cacheStrategy.isRecordingStats());
		} else {                                        // LRU
			return createLRUCache(
					tableName, cacheStrategy.getSize(), cacheStrategy.getTtlMs(), cacheStrategy.isRecordingStats());
		}
	}

	public synchronized void removeCache(String tableName) {
		cacheMap.remove(tableName);
	}

	public void clearAll() {
		synchronized (cacheMap) {
			if (null != cacheMap && cacheMap.size() > 0) {
				cacheMap.clear();
			}
		}
	}

	private Cache<K, V> createNoneCache(String tableName) {
		Cache<K, V> cache = cacheMap.get(tableName);
		if (cache == null) {
			cache = new NoneCache<>();
			cacheMap.put(tableName, cache);
			LOG.info("Create None cache for table: " + tableName);
		}
		return cache;
	}

	private Cache<K, V> createAllCache(String tableName, long ttlMs) {
		Cache<K, V> cache = cacheMap.get(tableName);
		if (cache == null) {
			cache = new AllCache<>();
			cacheMap.put(tableName, cache);
			LOG.info("Create AllCache, tableName=" + tableName + ", reloadInterval=" + ttlMs + "ms.");
		} else {
			LOG.info("Get AllCache from factory, tableName=" + tableName + ", reloadInterval=" + ttlMs + "ms.");
		}
		return cache;
	}

	private Cache<K, V> createLRUCache(String tableName, long cacheSize, boolean isRecordingStats) {
		checkArgument(cacheSize > 0, "cache must be greater than zero");

		Cache<K, V> lruCache = cacheMap.get(tableName);
		if (lruCache == null) {
			lruCache = new LRUCache<>(cacheSize, isRecordingStats);
			cacheMap.put(tableName, lruCache);
			LOG.info("Create LRUCache, tableName=" + tableName + ", cacheSize=" + cacheSize + ", never expired.");
		} else {
			LOG.info("Get LRUCache from factory, tableName=" + tableName + ", cacheSize=" + cacheSize + ", never " +
					"expired.");
		}
		return lruCache;
	}

	private Cache<K, V> createLRUCache(
			String tableName,
			long cacheSize,
			long cacheExpireTime,
			boolean isRecordingStats) {
		checkArgument(cacheSize > 0, "cache size must be greater than zero");
		checkArgument(cacheExpireTime > 0, "cache expire time must be greater than zero");

		Cache<K, V> lruCache = cacheMap.get(tableName);
		if (lruCache == null) {
			lruCache = new LRUCache<>(cacheSize, cacheExpireTime, isRecordingStats);
			cacheMap.put(tableName, lruCache);
			LOG.info("Create LRUCache, tableName=" + tableName + ", cacheSize=" + cacheSize + ", expireTime=" +
					cacheExpireTime);
		} else {
			LOG.info("Get LRUCache from factory, tableName=" + tableName +
					", cacheSize=" + cacheSize + ", expireTime=" + cacheExpireTime);
		}
		return lruCache;
	}
}
