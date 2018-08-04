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

import javax.annotation.Nullable;

/**
 * RocksDB state iterator with given prefix key-bytes.
 */
abstract class RocksDBStatePrefixIterator<T> extends AbstractRocksDBStateIterator<T> {

	/** The prefix bytes for this iterator to ensure all keys in this iterator starts with this byte array. */
	@Nullable
	private final byte[] dbKeyPrefix;

	RocksDBStatePrefixIterator(RocksDBInstance dbInstance, byte[] dbKeyPrefix) {
		super(dbInstance);
		this.dbKeyPrefix = dbKeyPrefix;
	}

	@Override
	byte[] getStartDBKey() {
		return dbKeyPrefix;
	}

	@Override
	boolean isEndDBKey(byte[] dbKey) {
		Preconditions.checkArgument(dbKey != null);

		return dbKeyPrefix != null &&
			!RocksDBInstance.isPrefixWith(dbKey, dbKeyPrefix);
	}
}
