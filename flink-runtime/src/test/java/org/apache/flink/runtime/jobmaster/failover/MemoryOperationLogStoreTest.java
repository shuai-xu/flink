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

package org.apache.flink.runtime.jobmaster.failover;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class MemoryOperationLogStoreTest {
	@Test
	public void testReadAndWriteOpLog() throws IOException {
		OperationLogStore store = new MemoryOperationLogStore();

		// Initial the store and test read write.
		store.start();

		OperationLog opLog0 = new TestingOperationLog(0);
		OperationLog opLog1 = new TestingOperationLog(1);
		OperationLog opLog2 = new TestingOperationLog(2);
		OperationLog opLog3 = new TestingOperationLog(3);
		OperationLog opLog4 = new TestingOperationLog(4);

		store.writeOpLog(opLog0);
		store.writeOpLog(opLog1);
		store.writeOpLog(opLog2);
		store.writeOpLog(opLog3);
		store.writeOpLog(opLog4);

		Iterator<OperationLog> opLogs = store.opLogs().iterator();

		Assert.assertEquals(opLog0, opLogs.next());
		Assert.assertEquals(opLog1, opLogs.next());
		Assert.assertEquals(opLog2, opLogs.next());
		Assert.assertEquals(opLog3, opLogs.next());
		Assert.assertEquals(opLog4, opLogs.next());
		Assert.assertFalse(opLogs.hasNext());

		store.stop();

		// test restart a fs store can retrieve former logs
		store.start();

		opLogs = store.opLogs().iterator();

		Assert.assertEquals(opLog0, opLogs.next());
		Assert.assertEquals(opLog1, opLogs.next());
		Assert.assertEquals(opLog2, opLogs.next());
		Assert.assertEquals(opLog3, opLogs.next());
		Assert.assertEquals(opLog4, opLogs.next());
		Assert.assertFalse(opLogs.hasNext());

		store.stop();

		// test clear the store
		store.start();
		store.clear();

		store.start();

		opLogs = store.opLogs().iterator();
		Assert.assertFalse(opLogs.hasNext());

		store.stop();
	}
}
