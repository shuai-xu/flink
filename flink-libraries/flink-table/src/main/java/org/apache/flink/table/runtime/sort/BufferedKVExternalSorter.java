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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.CompressedHeaderlessChannelWriterOutputView;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.table.util.BinaryMergeIterator;
import org.apache.flink.table.util.ChannelWithMeta;
import org.apache.flink.table.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import static org.apache.flink.table.runtime.sort.BinaryExternalMerger.getSegmentsForReaders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Sorter for buffered input in the form of Key-Value Style.
 * First, sort and spill buffered inputs.
 * Second, merge disk outputs and return iterator.
 */
public class BufferedKVExternalSorter {

	private static final Logger LOG = LoggerFactory.getLogger(BufferedKVExternalSorter.class);

	private volatile boolean closed = false;

	private static final int WRITE_MEMORY_NUM = 2;

	private final NormalizedKeyComputer nKeyComputer;
	private final RecordComparator comparator;
	private final BinaryRowSerializer keySerializer;
	private final BinaryRowSerializer valueSerializer;
	private final IndexedSorter sorter;

	private final BinaryKVExternalMerger merger;

	private final IOManager ioManager;
	private final int maxNumFileHandles;
	private final FileIOChannel.Enumerator enumerator;
	private final List<ChannelWithMeta> channelIDs = new ArrayList<>();
	private final SpillChannelManager channelManager;

	private final int writeNumMemory;

	private final boolean compressionEnable;
	private final String compressionCodec;
	private final int compressionBlockSize;

	public BufferedKVExternalSorter(
			IOManager ioManager,
			BinaryRowSerializer keySerializer,
			BinaryRowSerializer valueSerializer,
			NormalizedKeyComputer nKeyComputer,
			RecordComparator comparator,
			int pageSize,
			Configuration conf) throws IOException {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.nKeyComputer = nKeyComputer;
		this.comparator = comparator;
		this.sorter = new QuickSort();
		this.maxNumFileHandles = conf.getInteger(TableConfig.SQL_EXEC_SORT_MAX_NUM_FILE_HANDLES());
		this.compressionEnable = conf.getBoolean(TableConfig.SQL_EXEC_SPILL_COMPRESSION_ENABLE());
		this.compressionCodec = conf.getString(TableConfig.SQL_EXEC_SPILL_COMPRESSION_CODEC());
		this.compressionBlockSize = conf.getInteger(TableConfig.SQL_EXEC_SPILL_COMPRESSION_BLOCK_SIZE());
		this.writeNumMemory = compressionEnable ? 0 : WRITE_MEMORY_NUM;
		this.ioManager = ioManager;
		this.enumerator = this.ioManager.createChannelEnumerator();
		this.channelManager = new SpillChannelManager();
		this.merger = new BinaryKVExternalMerger(
				ioManager, pageSize,
				maxNumFileHandles, channelManager,
				keySerializer, valueSerializer, comparator,
				compressionEnable,
				compressionCodec,
				compressionBlockSize);
	}

	public MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> getKVIterator(
			MemorySegmentPool pool) throws IOException {
		return getKVIterator(new ArrayList<>(), pool);
	}

	public MutableObjectIterator<Tuple2<BinaryRow, BinaryRow>> getKVIterator(
			List<MemorySegment> memorySegments, MemorySegmentPool pool) throws IOException {

		int totalNeedSegs = writeNumMemory + Math.min(channelIDs.size(), maxNumFileHandles);
		int nowSegs = memorySegments.size();
		if (totalNeedSegs > nowSegs) {
			memorySegments = new ArrayList<>(memorySegments);
			for (int i = 0; i < totalNeedSegs - nowSegs; i++) {
				memorySegments.add(pool.nextSegment());
			}
		}

		// 1. merge if more than maxNumFile
		List<MemorySegment> writeMemory =
				new ArrayList<>(memorySegments.subList(0, writeNumMemory));
		List<MemorySegment> mergeReadMemory =
				new ArrayList<>(memorySegments.subList(writeNumMemory, memorySegments.size()));

		// merge channels until sufficient file handles are available
		List<ChannelWithMeta> channelIDs = this.channelIDs;
		while (!closed && channelIDs.size() > this.maxNumFileHandles) {
			channelIDs = merger.mergeChannelList(channelIDs, mergeReadMemory, writeMemory);
		}

		// 2. final merge
		// allocate the memory for the final merging step
		List<List<MemorySegment>> readBuffers = new ArrayList<>(channelIDs.size());

		// allocate the read memory and register it to be released
		getSegmentsForReaders(readBuffers, mergeReadMemory, channelIDs.size());

		List<FileIOChannel> openChannels = new ArrayList<>();
		BinaryMergeIterator<Tuple2<BinaryRow, BinaryRow>> iterator =
				merger.getMergingIterator(channelIDs, readBuffers, openChannels);
		channelManager.addOpenChannels(openChannels);

		return iterator;
	}

	public void sortAndSpill(
			ArrayList<MemorySegment> recordBufferSegments,
			long numElements,
			MemorySegmentPool pool) throws IOException {

		// 1. sort buffer
		BinaryKVInMemorySortBuffer buffer =
				BinaryKVInMemorySortBuffer.createBuffer(
						nKeyComputer, keySerializer, valueSerializer, comparator,
						recordBufferSegments, numElements, pool);
		this.sorter.sort(buffer);

		// 2. spill
		FileIOChannel.ID channel = enumerator.next();
		channelManager.addChannel(channel);

		// create writer
		FileIOChannel writer = null;
		int bytesInLastBuffer = -1;
		int blockCount;
		try {
			if (compressionEnable) {
				BufferFileWriter bufferWriter = this.ioManager.createBufferFileWriter(channel);
				writer = bufferWriter;
				CompressedHeaderlessChannelWriterOutputView output = new CompressedHeaderlessChannelWriterOutputView(
						bufferWriter, compressionCodec, compressionBlockSize);
				buffer.writeToOutput(output);
				output.close();
				blockCount = output.getBlockCount();
				LOG.info("here spill the kv external buffer data with {} bytes and {} compressed bytes",
						output.getNumBytes(), output.getNumCompressedBytes());
			} else {
				BlockChannelWriter<MemorySegment> blockWriter = this.ioManager.createBlockChannelWriter(channel);
				writer = blockWriter;
				HeaderlessChannelWriterOutputView output = new HeaderlessChannelWriterOutputView(
						blockWriter, Arrays.asList(pool.nextSegment(), pool.nextSegment()), pool.pageSize());
				buffer.writeToOutput(output);
				LOG.info("here spill the kv external buffer data with {} bytes", writer.getSize());
				bytesInLastBuffer = output.close();
				blockCount = output.getBlockCount();
			}
		} catch (IOException e) {
			if (writer != null) {
				writer.closeAndDelete();
			}
			throw e;
		}
		channelIDs.add(new ChannelWithMeta(channel, blockCount, bytesInLastBuffer));
	}

	public void close() {
		if (closed) {
			return;
		}
		// mark as closed
		closed = true;
		merger.close();
		channelManager.close();
	}
}
