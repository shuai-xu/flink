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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * In memory buffer that stores records to memorySegments, returns a iterator that map from memory.
 */
public class InMemoryBuffer {

	private final int segmentSize;
	private final ArrayList<MemorySegment> freeMemory;
	private final BinaryRowSerializer serializer;
	private final ArrayList<MemorySegment> recordBufferSegments;
	private final SimpleCollectingOutputView recordCollector;

	// Can't use recordCollector.getCurrentOffset(), maybe the offset of recordCollector is
	// disrupted by the attempt of record writing.
	private long currentDataBufferOffset;

	private int recordCount;

	public InMemoryBuffer(List<MemorySegment> memory, BinaryRowSerializer serializer) {
		this.segmentSize = memory.get(0).size();
		this.freeMemory = new ArrayList<>(memory);
		this.serializer = (BinaryRowSerializer) serializer.duplicate();
		this.recordBufferSegments = new ArrayList<>(memory.size());
		this.recordCollector = new SimpleCollectingOutputView(this.recordBufferSegments,
				new ListMemorySegmentSource(this.freeMemory), this.segmentSize);
		this.recordCount = 0;
	}

	public void reset() {
		this.currentDataBufferOffset = 0;
		this.recordCount = 0;

		// reset free and record segments.
		this.freeMemory.addAll(this.recordBufferSegments);
		this.recordBufferSegments.clear();

		this.recordCollector.reset();
	}

	public void clear() {
		this.freeMemory.clear();
		this.recordBufferSegments.clear();
	}

	public boolean write(BinaryRow row) throws IOException {
		try {
			this.serializer.serializeToPages(row, this.recordCollector);
			currentDataBufferOffset = this.recordCollector.getCurrentOffset();
			recordCount++;
			return true;
		} catch (EOFException e) {
			return false;
		}
	}

	public ArrayList<MemorySegment> getRecordBufferSegments() {
		return recordBufferSegments;
	}

	public long getCurrentDataBufferOffset() {
		return currentDataBufferOffset;
	}

	public int getNumRecordBuffers() {
		int result = (int) (currentDataBufferOffset / segmentSize);
		long mod = currentDataBufferOffset % segmentSize;
		if (mod != 0) {
			result += 1;
		}
		return result;
	}

	public int getNumBytesInLastBuffer() {
		int result = (int) (currentDataBufferOffset % segmentSize);
		if (result == 0) {
			result = segmentSize;
		}
		return result;
	}

	public final BufferIterator getIterator() {
		return getIterator(0, 0);
	}

	public final BufferIterator getIterator(int beginRow, long offset) {
		checkArgument(offset >= 0, "`offset` can't be negative!");

		RandomAccessInputView recordBuffer = new RandomAccessInputView(
				this.recordBufferSegments, this.segmentSize, getNumBytesInLastBuffer());
		return new BufferIterator(beginRow, offset, recordBuffer);
	}

	public ArrayList<MemorySegment> getFreeMemory() {
		return freeMemory;
	}

	/**
	 * Iterator of in memory buffer.
	 */
	public class BufferIterator implements MutableObjectIterator<BinaryRow> {
		private int count;
		private int beginRow;
		private long offset;
		private RandomAccessInputView recordBuffer;

		private BufferIterator(int beginRow, long offset, RandomAccessInputView recordBuffer) {
			this.beginRow = beginRow;
			this.offset = offset;
			this.recordBuffer = recordBuffer;
			reset();
		}

		public void reset() {
			this.count = beginRow;
			recordBuffer.setReadPosition(offset);
		}

		@Override
		public BinaryRow next(BinaryRow reuse) throws IOException {
			try {
				if (++count > recordCount) {
					return null;
				}
				return serializer.mapFromPages(reuse, recordBuffer);
			} catch (EOFException e) {
				return null;
			}
		}

		@Override
		public BinaryRow next() throws IOException {
			throw new RuntimeException("Not support!");
		}
	}
}
