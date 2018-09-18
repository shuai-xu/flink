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

import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.Serializable;

/**
 * Cache strategy.
 */
public class CacheStrategy implements Serializable {

	private static final long serialVersionUID = 5393301909928230796L;

	private final long size;
	private final long ttlMs;
	// cache an empty entry, true by default if not cacheAll, always false if cacheAll.
	private final boolean cacheEmpty;
	private final boolean cacheAll;
	private final boolean isRecordingStats;

	private CacheStrategy(long size, long ttlMs) {
		this(size, ttlMs, false, true);
	}

	private CacheStrategy(long size, long ttlMs, boolean isRecordingStats, boolean cacheEmpty) {
		checkArgument(size >= 0, "size must be greater or equal to zero");
		checkArgument(ttlMs >= 0, "ttlMs must be greater or equal to zero");
		this.size = size;
		this.ttlMs = ttlMs;
		this.cacheAll = false;
		this.cacheEmpty = cacheEmpty;
		this.isRecordingStats = isRecordingStats;
	}

	private CacheStrategy(boolean cacheAll, long ttlMs) {
		this.cacheAll = cacheAll;
		this.ttlMs = ttlMs;
		this.size = -1;
		this.isRecordingStats = false;
		this.cacheEmpty = false;
	}

	public boolean isRecordingStats() {
		return isRecordingStats;
	}

	public long getSize() {
		return size;
	}

	public long getTtlMs() {
		return ttlMs;
	}

	public boolean isNoCache() {
		return size == 0 || ttlMs == 0;
	}

	public boolean isNeverExpired() {
		return ttlMs == Long.MAX_VALUE;
	}

	public boolean isAllCache() {
		return cacheAll;
	}

	public boolean isCacheEmpty() {
		return cacheEmpty;
	}

	public static CacheStrategy lru(long cacheSize, long ttlMs, boolean isRecordingStats, boolean cacheEmpty) {
		return new CacheStrategy(cacheSize, ttlMs, isRecordingStats, cacheEmpty);
	}

	public static CacheStrategy lru(long cacheSize, long ttlMs, boolean isRecordingStats) {
		return new CacheStrategy(cacheSize, ttlMs, isRecordingStats, true);
	}

	public static CacheStrategy none() {
		return new CacheStrategy(0, 0);
	}

	public static CacheStrategy neverExpired(long cacheSize, boolean isRecordingStats, boolean cacheEmpty) {
		return new CacheStrategy(cacheSize, Long.MAX_VALUE, isRecordingStats, cacheEmpty);
	}

	public static CacheStrategy neverExpired(long cacheSize, boolean isRecordingStats) {
		return new CacheStrategy(cacheSize, Long.MAX_VALUE, isRecordingStats, true);
	}

	public static CacheStrategy all(long ttlMs) {
		return new CacheStrategy(true, ttlMs);
	}

	public static CacheStrategy all() {
		return new CacheStrategy(true, Long.MAX_VALUE);
	}
}
