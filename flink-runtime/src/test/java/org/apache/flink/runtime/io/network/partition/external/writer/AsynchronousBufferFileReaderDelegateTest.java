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

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class AsynchronousBufferFileReaderDelegateTest {
	private static final int MEMORY_SEGMENT_SIZE = 1024;

	private IOManager ioManager;

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
	}

	@Test
	public void testRead() throws IOException, InterruptedException {
		FileIOChannel.ID channel = ioManager.createChannel();
		BufferFileWriter writer = ioManager.createBufferFileWriter(channel);

		final int totalBuffers = 10;

		Random random = new Random();
		int next = 0;

		for (int i = 0; i < totalBuffers; ++i) {
			Buffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(MEMORY_SEGMENT_SIZE), new BufferRecycler() {
				@Override
				public void recycle(MemorySegment memorySegment) {
					// NOP
				}
			});

			int count = random.nextInt(MEMORY_SEGMENT_SIZE / 4);
			for (int j = 0; j < count; ++j) {
				buffer.getMemorySegment().putInt(j * 4, next++);
			}
			buffer.setSize(count * 4);

			writer.writeBlock(buffer);
		}
		writer.close();

		// now read it
		List<MemorySegment> memory = new ArrayList<>();
		for (int i = 0; i < 3; ++i) {
			memory.add(MemorySegmentFactory.allocateUnpooledSegment(MEMORY_SEGMENT_SIZE));
		}

		AsynchronousBufferFileReaderDelegate reader = new AsynchronousBufferFileReaderDelegate(ioManager, channel, memory, totalBuffers);

		int nextExpected = 0;
		for (int i = 0; i < totalBuffers; ++i) {
			Buffer buffer = reader.getNextBufferBlocking();
			int count = buffer.getSize() / 4;
			for (int j = 0; j < count; ++j) {
				int read = buffer.getMemorySegment().getInt(j * 4);
				Assert.assertEquals(nextExpected++, read);
			}
			buffer.recycleBuffer();
		}

		Assert.assertEquals(nextExpected, next);
	}

	@After
	public void after() {
		this.ioManager.shutdown();
	}
}
