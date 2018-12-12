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

package org.apache.flink.contrib.streaming.state3.keyed;

import org.apache.flink.contrib.streaming.state3.RocksDBInternalStateBackend;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state3.AbstractInternalStateBackend;
import org.apache.flink.runtime.state3.keyed.KeyedSortedMapStateTestBase;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

/**
 * Test for KeyedSortedMapState which uses RocksDB as the state backend.
 */
public class RocksDbKeyedSortedMapStateTest extends KeyedSortedMapStateTestBase {
	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Override
	public void openStateBackend() throws Exception {

		backend = new RocksDBInternalStateBackend(
			Thread.currentThread().getContextClassLoader(),
			temporaryFolder.newFolder().getAbsoluteFile(),
			new DBOptions().setCreateIfMissing(true),
			new ColumnFamilyOptions(),
			10,
			GroupRange.of(0, 10),
			true,
			new LocalRecoveryConfig(false, null),
			null);

		backend.restore(null);
	}

	@Override
	protected AbstractInternalStateBackend createStateBackend(int numberOfGroups, GroupSet groups, ClassLoader userClassLoader, LocalRecoveryConfig localRecoveryConfig) throws Exception {
		return backend;
	}
}

