/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A resettable external buffer for binary row. It stores records in memory and spill to disk
 * when memory is not enough. When the spill is completed, the records are written to memory.
 * The returned iterator reads the data in write order (read spilled records first).
 */
public class ResettableExternalBuffer implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(ResettableExternalBuffer.class);

	// We will only read one spilled file at the same time.
	static final int READ_BUFFER = 2;

	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final List<MemorySegment> memory;
	private final BinaryRowSerializer serializer;
	private final InMemoryBuffer inMemoryBuffer;

	private final List<ChannelWithMeta> channelIDs;

	// The size of each segment
	private int segmentSize;

	// The length of each row, if each row is of fixed length
	private long fixedLength;
	// If each row is of fixed length
	private boolean isFixedLength;

	private final List<Integer> numRowsUntilThisChannel;
	private int numRows = 0;

	// If an iterator is using the free memory of inMemoryBuffer
	private boolean freeMemoryInUse = false;
	// List of created iterator, will close all iterators when buffer is closed
	private final Set<BufferIterator> iterators;

	public ResettableExternalBuffer(
		MemoryManager memoryManager,
		IOManager ioManager,
		List<MemorySegment> memory,
		BinaryRowSerializer serializer) {
		this.memoryManager = memoryManager;
		this.ioManager = ioManager;
		this.memory = memory;
		this.serializer = serializer;
		this.inMemoryBuffer = new InMemoryBuffer(memory, serializer);
		this.channelIDs = new ArrayList<>();

		this.segmentSize = memory.get(0).size();

		this.numRowsUntilThisChannel = new ArrayList<>();
		this.isFixedLength = serializer.isRowFixedLength();
		if (this.isFixedLength) {
			this.fixedLength = serializer.getSerializedRowFixedPartLength();
		}

		iterators = new HashSet<>();
	}

	public int size() {
		return numRows;
	}

	private int memorySize() {
		return memory.size() * segmentSize;
	}

	public void add(BinaryRow row) throws IOException {
		if (!inMemoryBuffer.write(row)) {
			// Check if record is too big.
			if (inMemoryBuffer.getCurrentDataBufferOffset() == 0) {
				throw new IOException("Record can't be added to a empty InMemoryBuffer! " +
						"Record size: " + row.getSizeInBytes() + ", Buffer: " + memorySize());
			}
			spill();
			if (!inMemoryBuffer.write(row)) {
				throw new IOException("Record can't be added to a empty InMemoryBuffer! " +
						"Record size: " + row.getSizeInBytes() + ", Buffer: " + memorySize());
			}
		}

		numRows++;
	}

	private void spill() throws IOException {
		FileIOChannel.ID channel = ioManager.createChannel();

		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
		int numRecordBuffers = inMemoryBuffer.getNumRecordBuffers();
		ArrayList<MemorySegment> segments = inMemoryBuffer.getRecordBufferSegments();
		try {
			// spill in memory buffer in zero-copy.
			for (int i = 0; i < numRecordBuffers; i++) {
				writer.writeBlock(segments.get(i));
			}
			LOG.info("here spill the reset buffer data with {} bytes", writer.getSize());
			writer.close();
		} catch (IOException e) {
			writer.closeAndDelete();
			throw e;
		}

		channelIDs.add(new ChannelWithMeta(
				channel,
				inMemoryBuffer.getNumRecordBuffers(),
				inMemoryBuffer.getNumBytesInLastBuffer()));
		this.numRowsUntilThisChannel.add(numRows);

		inMemoryBuffer.reset();
	}

	public void reset() {
		clearChannels();
		inMemoryBuffer.reset();

		for (BufferIterator iterator : iterators) {
			iterator.closeImpl();
		}
		iterators.clear();

		numRows = 0;
	}

	@Override
	public void close() {
		clearChannels();
		memoryManager.release(memory);
		inMemoryBuffer.clear();

		for (BufferIterator iterator : iterators) {
			iterator.closeImpl();
		}
		iterators.clear();
	}

	private void clearChannels() {
		for (ChannelWithMeta meta : channelIDs) {
			final File f = new File(meta.getChannel().getPath());
			if (f.exists()) {
				f.delete();
			}
		}
		channelIDs.clear();

		numRowsUntilThisChannel.clear();
	}

	public BufferIterator newIterator() {
		return newIterator(0);
	}

	/**
	 * Get a new iterator starting from the `beginRow`-th row. `beginRow` is 0-indexed.
	 */
	public BufferIterator newIterator(int beginRow) {
		checkArgument(beginRow >= 0, "`beginRow` can't be negative!");

		BufferIterator iterator = new BufferIterator(beginRow);
		iterators.add(iterator);
		return iterator;
	}

	/**
	 * Iterator of external buffer.
	 */
	public class BufferIterator implements Closeable {

		MutableObjectIterator<BinaryRow> currentIterator;
		BlockChannelReader<MemorySegment> fileReader;
		int currentChannelID = -1;
		BinaryRow reuse = serializer.createInstance();
		BinaryRow row;
		int beginRow;
		// reuse in memory buffer iterator to reduce initialization cost.
		InMemoryBuffer.BufferIterator reusableMemoryIterator;

		List<MemorySegment> freeMemory = null;
		boolean usingMemBufFreeMemory = false;

		int nRows;
		boolean closed = false;

		private BufferIterator(int beginRow) {
			this.beginRow = Math.min(beginRow, numRows);
			nRows = numRows;

			if (needReadSpilled()) {
				// Only initialize freeMemory when we need to read spilled records.
				freeMemory = new ArrayList<>();
				// Iterator will first try to use free memory segments from inMemoryBuffer.
				if (!freeMemoryInUse) {
					List<MemorySegment> mem = inMemoryBuffer.getFreeMemory();
					if (mem.size() > 0) {
						freeMemory.addAll(mem);
						freeMemoryInUse = true;
						usingMemBufFreeMemory = true;
					}
				}
				// Iterator will use memory segments from heap
				// if free memory from inMemoryBuffer is not enough.
				for (int i = freeMemory.size(); i < READ_BUFFER; i++) {
					freeMemory.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
				}
			}
		}

		public void reset() throws IOException {
			validateIterator();

			closeCurrentFileReader();

			currentChannelID = -1;
			currentIterator = null;
			reuse.unbindMemorySegment();
		}

		@Override
		public void close() {
			if (!closed) {
				// Separate this method out to prevent concurrent modification
				// when buffer iterates through iterator set and close them.
				closeImpl();
				iterators.remove(this);
			}
		}

		private void closeImpl() {
			try {
				reset();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			if (usingMemBufFreeMemory) {
				freeMemoryInUse = false;
			}
			if (freeMemory != null) {
				freeMemory.clear();
			}

			closed = true;
		}

		private void validateIterator() {
			if (nRows != numRows) {
				throw new RuntimeException(
					"This buffer has been modified. This iterator is no longer valid.");
			}
			if (closed) {
				throw new RuntimeException("This iterator is closed.");
			}
		}

		public boolean advanceNext() {
			validateIterator();

			try {
				// get from curr iterator or new iterator.
				while (true) {
					if (currentIterator != null &&
							(row = currentIterator.next(reuse)) != null) {
						return true;
					} else {
						if (!nextIterator()) {
							return false;
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private boolean nextIterator() throws IOException, RuntimeException {
			if (currentChannelID == -1) {
				// First call to next iterator. Fetch iterator according to beginRow.
				if (isFixedLength) {
					firstFixedLengthIterator();
				} else {
					firstVariableLengthIterator();
				}
			} else if (currentChannelID == Integer.MAX_VALUE) {
				// The last one is in memory, so the end.
				return false;
			} else if (currentChannelID < channelIDs.size() - 1) {
				// Next spilled iterator.
				nextSpilledIterator();
			} else {
				// It is the last iterator.
				newMemoryIterator();
			}
			return true;
		}

		public BinaryRow getRow() {
			validateIterator();
			return row;
		}

		private void closeCurrentFileReader() throws IOException {
			if (fileReader != null) {
				fileReader.close();
				fileReader = null;
			}
		}

		private void firstFixedLengthIterator() throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, numRowsUntilThisChannel);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			// Fixed length. Calculate offset directly.
			long numRecordsInSegment = segmentSize / fixedLength;
			long offset =
				(beginRowInChannel / numRecordsInSegment) * segmentSize +
				(beginRowInChannel % numRecordsInSegment) * fixedLength;

			if (beginChannel < numRowsUntilThisChannel.size()) {
				// Data on disk
				newSpilledIterator(channelIDs.get(currentChannelID = beginChannel), offset);
			} else {
				// Data in memory
				newMemoryIterator(beginRowInChannel, offset);
			}
		}

		private void firstVariableLengthIterator() throws IOException {
			// Find which channel contains the row.
			int beginChannel = upperBound(beginRow, numRowsUntilThisChannel);
			// Find the row number in its own channel (0-indexed).
			int beginRowInChannel = getBeginIndexInChannel(beginRow, beginChannel);
			if (beginRow == numRows) {
				// Row number out of range! Should return an "empty" iterator.
				newMemoryIterator(beginRowInChannel, inMemoryBuffer.getCurrentDataBufferOffset());
				return;
			}

			if (beginChannel < numRowsUntilThisChannel.size()) {
				// Data on disk
				newSpilledIterator(channelIDs.get(currentChannelID = beginChannel));
				for (int i = 0; i < beginRowInChannel; i++) {
					advanceNext();
				}
			} else {
				// Data in memory
				newMemoryIterator();
				for (int i = 0; i < beginRowInChannel; i++) {
					advanceNext();
				}
			}
		}

		private void nextSpilledIterator() throws IOException {
			newSpilledIterator(channelIDs.get(++currentChannelID));
		}

		private void newSpilledIterator(ChannelWithMeta channel) throws IOException {
			newSpilledIterator(channel, 0);
		}

		private void newSpilledIterator(ChannelWithMeta channel, long offset) throws IOException {
			// close current reader first.
			closeCurrentFileReader();

			// calculate segment number
			int segmentNum = (int) (offset / segmentSize);
			long seekPosition = segmentNum * segmentSize;

			// new reader.
			this.fileReader = ioManager.createBlockChannelReader(channel.getChannel());
			if (offset > 0) {
				// seek to the beginning of that segment
				fileReader.seekToPosition(seekPosition);
			}
			ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(
					fileReader, freeMemory, channel.getBlockCount() - segmentNum,
					channel.getNumBytesInLastBlock(), false, offset - seekPosition
			);
			this.currentIterator = new PagedChannelReaderInputViewIterator<>(
				inView, null, serializer.duplicate()
			);
		}

		private void newMemoryIterator() throws IOException {
			newMemoryIterator(0, 0);
		}

		private void newMemoryIterator(int beginRow, long offset) throws IOException {
			currentChannelID = Integer.MAX_VALUE;
			// close curr reader first.
			closeCurrentFileReader();

			if (reusableMemoryIterator == null) {
				reusableMemoryIterator = inMemoryBuffer.getIterator(beginRow, offset);
			} else {
				reusableMemoryIterator.reset();
			}
			this.currentIterator = reusableMemoryIterator;
		}

		private int getBeginIndexInChannel(int beginRow, int beginChannel) {
			if (beginChannel > 0) {
				return beginRow - numRowsUntilThisChannel.get(beginChannel - 1);
			} else {
				return beginRow;
			}
		}

		private boolean needReadSpilled() {
			int beginChannel = upperBound(beginRow, numRowsUntilThisChannel);
			return beginChannel < numRowsUntilThisChannel.size();
		}

		// Find the index of the first element which is strictly greater than `goal` in `list`.
		// `list` must be sorted.
		// If every element in `list` is not larger than `goal`, return `list.size()`.
		private int upperBound(int goal, List<Integer> list) {
			if (list.size() == 0) {
				return 0;
			}
			if (list.get(list.size() - 1) <= goal) {
				return list.size();
			}

			// Binary search
			int head = 0;
			int tail = list.size() - 1;
			int mid;
			while (head < tail) {
				mid = (head + tail) / 2;
				if (list.get(mid) <= goal) {
					head = mid + 1;
				} else {
					tail = mid;
				}
			}
			return head;
		}
	}

	@VisibleForTesting
	List<ChannelWithMeta> getSpillChannels() {
		return channelIDs;
	}
}
