/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.operators.StreamOperatorSnapshotRestoreTest;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Test snapshot/restore of stream operators for Niagara (full snapshots).
 * TODO re-enable this UT after keyed state backend is replaced by internal state backend.
 */
@Ignore
public class NiagaraStreamOperatorSnapshotRestoreTest extends StreamOperatorSnapshotRestoreTest {

	@BeforeClass
	public static void platformCheck() {
		// Check OS & Kernel version before run unit tests
		Assume.assumeTrue(System.getProperty("os.name").startsWith("Linux") && System.getProperty("os.version").contains("alios7"));
	}

	@Override
	protected StateBackend createStateBackend() throws IOException {
		FsStateBackend stateBackend = createStateBackendInternal();
		return new NiagaraStateBackend(stateBackend, TernaryBoolean.FALSE, new NiagaraConfiguration());
	}
}
