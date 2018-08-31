/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external.writer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * Temporary test, will be revised later.
 */
public class ExternalBlockShuffleWriterTest {
	public static final int PAGE_SIZE = 4096;

	public static final int NUM_PAGES = 100;

	public static final int MEMORY_SIZE = PAGE_SIZE * NUM_PAGES;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private IOManager ioManager;
	private MemoryManager memoryManager;
	private TypeSerializer<Integer> serializer;

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, 4096, MemoryType.HEAP, true);
		this.serializer = IntSerializer.INSTANCE;
	}

	@Test
	public void testWriting() throws IOException, MemoryAllocationException, InterruptedException {
		String partitionRootPath = temporaryFolder.newFolder().getAbsolutePath() + "/";

		PersistentFileWriter<Integer> shuffleWriter = new HashPartitionFileWriter<>(
			4, partitionRootPath, memoryManager, memoryManager.allocatePages(this, NUM_PAGES),
			serializer, ioManager);

		final int maxNumber = 10000;
		for (int i = 0; i < maxNumber; ++i) {
			shuffleWriter.add(i, new int[]{0, 1, 2, 3});
		}

		shuffleWriter.finishAddingRecords();
		List<List<PartitionIndex>> partitionIndices = shuffleWriter.generatePartitionIndices();

		for (int i = 0; i < 4; ++i) {
			String filePath = ExternalBlockShuffleUtils.generateDataPath(partitionRootPath, i);

			BufferSortedDataFileReader<Integer> bufferSortedDataFileReader = new BufferSortedDataFileReader<>(
				filePath,
				temporaryFolder.newFolder().getAbsolutePath(),
				ioManager,
				PAGE_SIZE,
				serializer);

			Integer result;
			int nextExpected = 0;

			while ((result = bufferSortedDataFileReader.next()) != null) {
				assertEquals(nextExpected, result.intValue());
				nextExpected++;
			}

			assertEquals(maxNumber, nextExpected);
		}
	}
}
