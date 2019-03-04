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

package org.apache.flink.table.runtime.window.aligned;

import org.apache.flink.table.runtime.window.aligned.BufferedAlignedWindowAggregator.BufferEntry;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for BufferedAlignedWindowAggregator.
 */
public class BufferedAlignedWindowAggregatorTest {

	@Test
	public void testBufferEntryStatusBitConversion() {
		BufferEntry[] bufferEntries = new BufferEntry[] {
			new BufferEntry(null, true, false, false),
			new BufferEntry(null, true, false, true),
			new BufferEntry(null, true, true, false),
			new BufferEntry(null, true, true, true),
			new BufferEntry(null, false, false, false),
			new BufferEntry(null, false, false, true),
			new BufferEntry(null, false, true, false),
			new BufferEntry(null, false, true, true),
		};
		for (BufferEntry bufferEntry : bufferEntries) {
			checkBitConversion(bufferEntry);
		}
	}

	private void checkBitConversion(BufferEntry bufferEntry) {
		BufferEntry newEntry = BufferEntry.of(null);
		newEntry.setStatus(bufferEntry.getStatus());
		assertEquals(newEntry, bufferEntry);
	}

}
