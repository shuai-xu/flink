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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.Preconditions;

/**
 * RocksDB state iterator with given start key-bytes and end key-bytes, which means the created {@link RocksDBStateRangeIterator}
 * containing keys from [startDBKey, endDBKey] if they all existed.
 */
abstract class RocksDBStateRangeIterator<T> extends AbstractRocksDBStateIterator<T> {

	/** The inclusive start key-bytes. */
	private final byte[] startDBKey;

	/** The exclusive end key-bytes. */
	private final byte[] endDBKey;

	RocksDBStateRangeIterator(
		RocksDBInstance dbInstance,
		byte[] startDBKey,
		byte[] endDBKey
	) {
		super(dbInstance);
		Preconditions.checkArgument(startDBKey != null,
			"start key bytes cannot be null when creating RocksDBStateRangeIterator.");
		Preconditions.checkArgument(endDBKey != null,
			"end key bytes cannot be null when creating RocksDBStateRangeIterator.");
		Preconditions.checkArgument(RocksDBInstance.compare(startDBKey, endDBKey) <= 0,
			" start key bytes must order before end key bytes when creating RocksDBStateRangeIterator.");

		this.startDBKey = startDBKey;
		this.endDBKey = endDBKey;
	}

	@Override
	byte[] getStartDBKey() {
		return startDBKey;
	}

	@Override
	boolean isEndDBKey(byte[] dbKey) {
		Preconditions.checkArgument(dbKey != null);

		return RocksDBInstance.compare(dbKey, endDBKey) >= 0;
	}

}
