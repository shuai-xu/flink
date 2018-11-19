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

package com.alibaba.blink.state.niagara.subkeyed;

import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateTest;

import com.alibaba.blink.state.niagara.NiagaraStateBackend;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SubKeyedValueState} backed by {@link NiagaraStateBackend}.
 */
public class NiagaraSubKeyedValueStateTest extends SubKeyedValueStateTest {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Override
	public void openStateBackend() throws Exception {
		NiagaraStateBackend niagaraStateBackend = new NiagaraStateBackend(temporaryFolder.newFolder().toURI().toString(), true);
		niagaraStateBackend.setDbStoragePath(temporaryFolder.newFolder().toString());
		backend = niagaraStateBackend.createInternalStateBackend(
			new DummyEnvironment(),
			"test_op",
			10,
			new GroupRange(0, 10));

		backend.restore(null);
	}
}
