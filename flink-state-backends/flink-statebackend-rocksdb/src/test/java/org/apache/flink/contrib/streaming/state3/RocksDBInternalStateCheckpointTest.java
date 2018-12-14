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

package org.apache.flink.contrib.streaming.state3;

import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state3.InternalStateBackend;
import org.apache.flink.runtime.state3.InternalStateCheckpointTestBase;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test for checkpoint with RocksDB state backend.
 */
@RunWith(Parameterized.class)
public class RocksDBInternalStateCheckpointTest extends InternalStateCheckpointTestBase {
	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameterized.Parameter
	public CheckpointType checkpointType;

	@Parameterized.Parameters(name = "checkpointType: {0}")
	public static Collection<CheckpointType> checkpointTypes() {
		return Arrays.asList(CheckpointType.values());
	}

	@Override
	protected InternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) throws Exception {
		InternalStateBackend backend = new RocksDBInternalStateBackend(
			Thread.currentThread().getContextClassLoader(),
			temporaryFolder.newFolder().getAbsoluteFile(),
			new DBOptions().setCreateIfMissing(true),
			new ColumnFamilyOptions(),
			numberOfGroups,
			groups,
			true,
			localRecoveryConfig,
			null);
		return backend;
	}

	@Override
	protected CheckpointType getCheckpointType() {
		return checkpointType;
	}
}

