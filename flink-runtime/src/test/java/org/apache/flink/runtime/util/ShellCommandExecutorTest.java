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

package org.apache.flink.runtime.util;

import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * A JUnit test to test {@link ShellCommandExecutor}.
 */
public class ShellCommandExecutorTest extends TestLogger {

	@Before
	public void setup() {
		assumeFalse(OperatingSystem.isWindows());
	}

	@Test
	public void testExecutionSuccessful() throws Exception {
		final ShellCommandExecutor shellCommandExecutor = new ShellCommandExecutor(new String[] {
			"echo", "Hello world"
		});

		shellCommandExecutor.execute();

		assertEquals(0, shellCommandExecutor.getExitCode());

		assertEquals("Hello world\n", shellCommandExecutor.getOutput());
	}

	@Test
	public void testExecutionFailed() throws Exception {
		final ShellCommandExecutor shellCommandExecutor = new ShellCommandExecutor(new String[] {
			"false"
		});

		shellCommandExecutor.execute();

		assertNotEquals(0, shellCommandExecutor.getExitCode());

		assertTrue(shellCommandExecutor.getOutput().isEmpty());
	}
}
