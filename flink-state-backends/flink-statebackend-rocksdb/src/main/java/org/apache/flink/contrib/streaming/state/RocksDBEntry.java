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

import org.apache.flink.util.Preconditions;

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
