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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.FixedLengthBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.verification.Timeout;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test the view of external result partition.
 */
@RunWith(Parameterized.class)
public class ExternalBlockSubpartitionViewTest {
	private static final int SEGMENT_SIZE = 128;

	private static final int NUM_BUFFERS = 20;

	private static final int[] TOTAL_BUFFERS_EACH_SUBPARTITION = new int[]{10, 20, 30, 40};

	private static final int MERGED_FILE_TOTAL_FILES = 5;

	private IOManager ioManager;

	private FixedLengthBufferPool bufferPool;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private final PersistentFileType fileType;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			/** Normal cases */
			{PersistentFileType.HASH_PARTITION_FILE},
			{PersistentFileType.MERGED_PARTITION_FILE},
		});
	}

	public ExternalBlockSubpartitionViewTest(PersistentFileType fileType) throws Exception {
		this.fileType = fileType;
	}

	@Before
	public void before() {
		this.ioManager = new IOManagerAsync();
		this.bufferPool = new FixedLengthBufferPool(NUM_BUFFERS, SEGMENT_SIZE, MemoryType.HEAP);
	}

	@After
	public void after() {
		ioManager.shutdown();

		assertEquals("Not all buffers returned", NUM_BUFFERS, bufferPool.getNumberOfAvailableMemorySegments());
		this.bufferPool.lazyDestroy();
	}

	@Test
	public void testInitializedOnFirstRead() throws Exception {
		final int subpartitionIndex = 2;

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());
		ExecutorService executor = Executors.newFixedThreadPool(1);

		ViewReader viewReader = new ViewReader();
		ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
			subpartitionIndex,
			executor,
			meta.getResultPartitionID(),
			bufferPool,
			0,
			viewReader);
		viewReader.setView(view);

		view.notifyCreditAdded(2);
		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 0);
		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 1);

		verify(meta).initialize();
		assertEquals(TOTAL_BUFFERS_EACH_SUBPARTITION[subpartitionIndex], view.getTotalBuffers());
		assertNotNull(view.getMetaIterator());
	}

	@Test(timeout = 60000)
	public void testManagingCredit() throws Exception {
		final int subpartitionIndex = 2;

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());
		ExecutorService executor = spy(Executors.newFixedThreadPool(1));

		ViewReader viewReader = new ViewReader();
		ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
			subpartitionIndex,
			executor,
			meta.getResultPartitionID(),
			bufferPool,
			0,
			viewReader);
		viewReader.setView(view);

		view.notifyCreditAdded(2);
		verify(executor).submit(eq(view));
		reset(executor);

		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 0);
		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 1);

		assertEquals(0, view.getCurrentCredit());
		view.notifyCreditAdded(2);
		verify(executor).submit(eq(view));

		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 2);
		checkBufferAndRecycle(viewReader.getNextBufferBlocking(), 3);
	}

	@Test(timeout = 60000)
	public void testGetNextBuffer() throws Exception {
		final int subpartitionIndex = 2;

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());
		ExecutorService executor = spy(Executors.newFixedThreadPool(1));

		ViewReader viewReader = new ViewReader();
		ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(meta,
			subpartitionIndex,
			executor,
			meta.getResultPartitionID(),
			bufferPool,
			0,
			viewReader);
		viewReader.setView(view);

		Random random = new Random();
		int nextBufferIndex = 0;

		int remainBuffer = TOTAL_BUFFERS_EACH_SUBPARTITION[subpartitionIndex];
		while (remainBuffer > 1) {
			int nextCredit = random.nextInt(remainBuffer - 1) + 1;
			view.notifyCreditAdded(nextCredit);

			for (int i = 0; i < nextCredit; ++i) {
				checkBufferAndRecycle(viewReader.getNextBufferBlocking(), nextBufferIndex++);

				if (i == nextCredit - 1) {
					assertFalse(view.isAvailable());
				}
			}

			remainBuffer -= nextCredit;
		}

		view.notifyCreditAdded(remainBuffer);
		for (int i = 0; i < remainBuffer; ++i) {
			checkBufferAndRecycle(viewReader.getNextBufferBlocking(), nextBufferIndex++);
		}

		Buffer eof = viewReader.getNextBufferBlocking();
		assertFalse(eof.isBuffer());
		assertEquals(EndOfPartitionEvent.INSTANCE, EventSerializer.fromBuffer(eof, this.getClass().getClassLoader()));
		assertFalse(view.isAvailable());
	}

	@Test
	public void testReadFail() throws Exception {
		final int subpartitionIndex = 2;

		ExternalBlockResultPartitionMeta meta = spy(createFilesAndMeta());
		ExecutorService executor = spy(Executors.newFixedThreadPool(1));

		BufferAvailabilityListener availabilityListener = mock(BufferAvailabilityListener.class);
		ExternalBlockSubpartitionView view = spy(new ExternalBlockSubpartitionView(meta,
			subpartitionIndex,
			executor,
			meta.getResultPartitionID(),
			bufferPool,
			0,
			availabilityListener));

		// Remove the data files directly
		int numFilesToRemove = (fileType == PersistentFileType.HASH_PARTITION_FILE ? TOTAL_BUFFERS_EACH_SUBPARTITION.length : MERGED_FILE_TOTAL_FILES);
		for (int i = 0; i < numFilesToRemove; ++i) {
			boolean success =
				new File(ExternalBlockShuffleUtils.generateDataPath(meta.getResultPartitionDir(), i)).delete();
			assertTrue("Delete the data file failed", success);
		}

		view.notifyCreditAdded(1);

		// Should be notified in expected period.
		verify(availabilityListener, timeout(10000)).notifyDataAvailable();

		assertTrue(view.nextBufferIsEvent());
		assertNull(view.getNextBuffer());
		assertNotNull(view.getFailureCause());
	}

	// -------------------------------- Internal Utilities ------------------------------------

	private ExternalBlockResultPartitionMeta createFilesAndMeta() throws Exception {
		if (fileType == PersistentFileType.HASH_PARTITION_FILE) {
			return createHashFilesAndMeta(TOTAL_BUFFERS_EACH_SUBPARTITION);
		} else {
			int[] buffersPerFile = new int[TOTAL_BUFFERS_EACH_SUBPARTITION.length];
			for (int i = 0; i < TOTAL_BUFFERS_EACH_SUBPARTITION.length; ++i) {
				buffersPerFile[i] = TOTAL_BUFFERS_EACH_SUBPARTITION[i] / MERGED_FILE_TOTAL_FILES;
			}

			return createMergeFilesAndMeta(buffersPerFile, MERGED_FILE_TOTAL_FILES);
		}
	}

	private ExternalBlockResultPartitionMeta createMergeFilesAndMeta(int[] buffersPerFileOfEachSubpartition, int numFiles) throws Exception {
		String root = tempFolder.newFolder().getAbsolutePath() + "/";
		FileSystem fs = FileSystem.getLocalFileSystem();

		List<List<PartitionIndex>> partitionIndicesList = new ArrayList<>();

		for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
			int bytesWritten = 0;
			List<PartitionIndex> partitionIndices = new ArrayList<>();

			BufferFileWriter writer = ioManager.createBufferFileWriter(
				new FileIOChannel.ID(ExternalBlockShuffleUtils.generateDataPath(root, fileIndex)));

			for (int i = 0; i < buffersPerFileOfEachSubpartition.length; ++i) {
				partitionIndices.add(new PartitionIndex(i, bytesWritten, buffersPerFileOfEachSubpartition[i]));

				for (int j = 0; j < buffersPerFileOfEachSubpartition[i]; ++j) {
					MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
					NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
					buffer.asByteBuf().writeInt(fileIndex * buffersPerFileOfEachSubpartition[i] + j);
					writer.writeBlock(buffer);

					bytesWritten = bytesWritten + BufferFileWriter.BUFFER_HEAD_LENGTH + buffer.getSize();
				}
			}

			writer.close();
			partitionIndicesList.add(partitionIndices);
		}

		// write index and finish files
		for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
			String indexPath = ExternalBlockShuffleUtils.generateIndexPath(root, fileIndex);
			try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
				DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
				ExternalBlockShuffleUtils.serializeIndices(partitionIndicesList.get(fileIndex), indexView);
			}
		}

		String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(root);
		try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

			finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

			String externalFileType = PersistentFileType.MERGED_PARTITION_FILE.toString();
			finishedView.writeInt(externalFileType.length());
			finishedView.write(externalFileType.getBytes());
			finishedView.writeInt(numFiles);
			finishedView.writeInt(buffersPerFileOfEachSubpartition.length);
		}

		return new ExternalBlockResultPartitionMeta(new ResultPartitionID(), fs, root, root);
	}

	private ExternalBlockResultPartitionMeta createHashFilesAndMeta(int[] buffersEachSubpartition) throws Exception {
		String root = tempFolder.newFolder().getAbsolutePath() + "/";
		FileSystem fs = FileSystem.getLocalFileSystem();

		for (int i = 0; i < buffersEachSubpartition.length; ++i) {
			BufferFileWriter writer = ioManager.createBufferFileWriter(
				new FileIOChannel.ID(ExternalBlockShuffleUtils.generateDataPath(root, i)));

			for (int j = 0; j < buffersEachSubpartition[i]; ++j) {
				MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
				NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
				buffer.asByteBuf().writeInt(j);
				writer.writeBlock(buffer);
			}

			writer.close();
		}

		// write index and finish files
		List<PartitionIndex> partitionIndexList = new ArrayList<>();
		for (int i = 0; i < buffersEachSubpartition.length; ++i) {
			partitionIndexList.add(new PartitionIndex(i, 0, buffersEachSubpartition[i]));
		}
		String indexPath = ExternalBlockShuffleUtils.generateIndexPath(root, 0);
		try (FSDataOutputStream indexOut = fs.create(new Path(indexPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView indexView = new DataOutputViewStreamWrapper(indexOut);
			ExternalBlockShuffleUtils.serializeIndices(partitionIndexList, indexView);
		}

		String finishedPath = ExternalBlockShuffleUtils.generateFinishedPath(root);
		try (FSDataOutputStream finishedOut = fs.create(new Path(finishedPath), FileSystem.WriteMode.OVERWRITE)) {
			DataOutputView finishedView = new DataOutputViewStreamWrapper(finishedOut);

			finishedView.writeInt(ExternalBlockResultPartitionMeta.SUPPORTED_PROTOCOL_VERSION);

			String externalFileType = PersistentFileType.HASH_PARTITION_FILE.toString();
			finishedView.writeInt(externalFileType.length());
			finishedView.write(externalFileType.getBytes());
			finishedView.writeInt(1);
			finishedView.writeInt(buffersEachSubpartition.length);
		}

		return new ExternalBlockResultPartitionMeta(new ResultPartitionID(), fs, root, root);
	}

	private void checkBufferAndRecycle(Buffer buffer, int expectedIndex) {
		assertTrue(buffer.isBuffer());
		assertEquals(expectedIndex, buffer.asByteBuf().readInt());
		buffer.recycleBuffer();
	}

	private class ViewReader implements BufferAvailabilityListener {
		private final BlockingQueue<Buffer> bufferRead = new LinkedBlockingQueue<>();
		private ExternalBlockSubpartitionView view;

		public void setView(ExternalBlockSubpartitionView view) {
			this.view = view;
		}

		@Override
		public void notifyDataAvailable() {
			while (true) {
				ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
				bufferRead.add(bufferAndBacklog.buffer());

				if (!bufferAndBacklog.isMoreAvailable()) {
					break;
				}
			}
		}

		public Buffer getNextBufferBlocking() throws InterruptedException {
			return bufferRead.take();
		}
	}
}
