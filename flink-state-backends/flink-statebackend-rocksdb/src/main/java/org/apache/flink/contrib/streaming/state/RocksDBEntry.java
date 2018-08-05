/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;

/**
 * The rocksDB entry of (dbKey, dbValue).
 */
public class RocksDBEntry {

	private final RocksDBInstance dbInstance;

	private final byte[] dbKey;

	private byte[] dbValue;

	private boolean deleted;

	RocksDBEntry(final RocksDBInstance dbInstance, final byte[] dbKey, final byte[] dbValue) {
		Preconditions.checkArgument(dbInstance != null);
		Preconditions.checkArgument(dbKey != null);
		Preconditions.checkArgument(dbValue != null);

		this.dbInstance = dbInstance;
		this.dbKey = dbKey;
		this.dbValue = dbValue;
		this.deleted = false;
	}

	void remove() {
		Preconditions.checkState(!deleted);

		deleted = true;
		dbValue = null;
		dbInstance.delete(dbKey);
	}

	/**
	 * Get the pair of row with given internal state descriptor.
	 *
	 * @param descriptor the given internal state descriptor
	 */
	Pair<Row, Row> getRowPair(InternalStateDescriptor descriptor) {
		Preconditions.checkNotNull(descriptor,
			"Must provide internal state descriptor to get the pair of row");

		return new Pair<Row, Row>() {
			/** The key of the pair. */
			private Row key;

			/** The value of the pair. */
			private Row value;

			@Override
			public Row getKey() {
				if (key == null) {
					key = RocksDBInternalState.deserializeStateKey(dbKey, descriptor);
				}
				return key;
			}

			@Override
			public Row getValue() {
				if (dbValue == null) {
					return null;
				}
				if (value == null) {
					if (descriptor.getValueMerger() != null) {
						List<Row> rows = RocksDBInternalState.deserializeStateValues(dbValue, descriptor);
						value = RocksDBInternalState.mergeMultiValues(rows, descriptor.getValueMerger());
					} else {
						value = RocksDBInternalState.deserializeStateValue(dbValue, descriptor);
					}
				}
				return value;
			}

			@Override
			public Row setValue(Row value) {
				throw new UnsupportedOperationException();
			}
		};
	}

	byte[] getDBKey() {
		return dbKey;
	}

	byte[] getDBValue() {
		return dbValue;
	}

	boolean isDeleted() {
		return deleted;
	}
}
