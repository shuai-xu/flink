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

package com.alibaba.blink.state.niagara;

import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalStateAccessTestBase;
import org.apache.flink.runtime.state.InternalStateBackend;
import org.apache.flink.runtime.state.LocalRecoveryConfig;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Unit tests to validate that internal states can be correctly accessed in
 * {@link NiagaraInternalStateBackend}.
 */
public class NiagaraInternalStateAccessTest extends InternalStateAccessTestBase {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static String niagaraLoadPath;

	@BeforeClass
	public static void setupNiagaraLoadPath() throws IOException {
		// Check OS & Kernel version before run unit tests
		Assume.assumeTrue(System.getProperty("os.name").startsWith("Linux") && System.getProperty("os.version").contains("alios7"));

		niagaraLoadPath = temporaryFolder.newFolder().getAbsolutePath();
	}

	@AfterClass
	public static void resetNiagara() throws Exception {
		NiagaraUtils.resetNiagaraLoadedFlag();
	}

	@Override
	protected InternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) throws Exception {

		NiagaraUtils.ensureNiagaraIsLoaded(niagaraLoadPath);
		return new NiagaraInternalStateBackend(
			userClassLoader,
			temporaryFolder.newFolder().getAbsoluteFile(),
			new NiagaraConfiguration(),
			numberOfGroups,
			groups,
			true,
			localRecoveryConfig,
			null);
	}
}
