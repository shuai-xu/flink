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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.PartitionIndex;
import org.apache.flink.runtime.operators.sort.ChannelDeleteRegistry;
import org.apache.flink.runtime.operators.sort.PartialOrderPriorityQueue;
import org.apache.flink.runtime.operators.sort.SortedDataFile;
import org.apache.flink.runtime.operators.sort.SortedDataFileMerger;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A merging policy who simply concats the part of the same partition together.
 */
public class ConcatPartitionedFileMerger<T> implements SortedDataFileMerger<Tuple2<Integer, T>> {
	private static final Logger LOG = LoggerFactory.getLogger(ConcatPartitionedFileMerger.class);

	private final int numberOfSubpartitions;
	private final String partitionDataRootPath;

	private final int maxDataFiles;
	private final int maxNumFileHandlesPerMerge;

	private final IOManager ioManager;

	ConcatPartitionedFileMerger(int numberOfSubpartitions, String partitionDataRootPath, int maxDataFiles, int maxNumFileHandlesPerMerge, IOManager ioManager) {
		this.numberOfSubpartitions = numberOfSubpartitions;
		this.partitionDataRootPath = partitionDataRootPath;

		this.maxDataFiles = maxDataFiles;
		this.maxNumFileHandlesPerMerge = maxNumFileHandlesPerMerge;

		this.ioManager = ioManager;
	}

	@Override
	public List<SortedDataFile<Tuple2<Integer, T>>> merge(List<SortedDataFile<Tuple2<Integer, T>>> channels, List<MemorySegment> writeMemory,
														  List<MemorySegment> mergeReadMemory, ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry, BooleanValue aliveFlag) throws IOException {
		LinkedList<PartitionedFileAndFileId<T>> candidates = new LinkedList<>();

		int nextSpillFileId = 0;

		for (int i = 0; i < channels.size(); ++i) {
			if (!(channels.get(i) instanceof PartitionedBufferSortedDataFile)) {
				throw new IllegalArgumentException("Only PartitionedBufferSortedDataFile supported");
			}

			PartitionedBufferSortedDataFile<T> partitionedSortedDataFile = (PartitionedBufferSortedDataFile<T>) channels.get(i);
			candidates.add(new PartitionedFileAndFileId<>(partitionedSortedDataFile, partitionedSortedDataFile.getFileId()));
			nextSpillFileId = Math.max(partitionedSortedDataFile.getFileId() + 1, nextSpillFileId);
		}

		// Parse indices and merge
		while (aliveFlag.getValue() && candidates.size() > maxDataFiles) {
			List<PartitionedFileAndFileId<T>> toBeMerge = new ArrayList<>();
			int maxMergeCount = Math.min(candidates.size() - maxDataFiles + 1, maxNumFileHandlesPerMerge);

			for (int i = 0; i < maxMergeCount; ++i) {
				PartitionedFileAndFileId<T> fileAndFileId = candidates.pollFirst();
				toBeMerge.add(fileAndFileId);

				if (candidates.size() > 0 && fileAndFileId.getFileId() > candidates.getFirst().getFileId()) {
					break;
				}
			}

			if (toBeMerge.size() == 1) {
				candidates.add(toBeMerge.get(0));
				continue;
			}

			LOG.info("Start merging {} files to one file, remaining files = {}", toBeMerge.size(), candidates.size());

			try {
				PartitionedFileAndFileId<T> mergedFile = mergeToOutput(toBeMerge, nextSpillFileId++, writeMemory, mergeReadMemory, channelDeleteRegistry);
				candidates.add(mergedFile);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		List<SortedDataFile<Tuple2<Integer, T>>> result = new ArrayList<>();
		for (PartitionedFileAndFileId<T> fileAndFileId : candidates) {
			result.add(fileAndFileId.getFile());
		}

		return result;
	}

	@Override
	public MutableObjectIterator<Tuple2<Integer, T>> getMergingIterator(List<SortedDataFile<Tuple2<Integer, T>>> sortedDataFiles, List<MemorySegment> mergeReadMemory, MutableObjectIterator<Tuple2<Integer, T>> largeRecords, ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry) throws IOException {
		//TODO: will be implemented later.
		return new MutableObjectIterator<Tuple2<Integer, T>>() {
			@Override
			public Tuple2<Integer, T> next(Tuple2<Integer, T> reuse) throws IOException {
				return null;
			}

			@Override
			public Tuple2<Integer, T> next() throws IOException {
				return null;
			}
		};
	}

	private PartitionedFileAndFileId mergeToOutput(List<PartitionedFileAndFileId<T>> toBeMerged, int fileId,
													List<MemorySegment> writeMemory, List<MemorySegment> mergeReadMemory,
													ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry) throws IOException, InterruptedException {
		// Create merged file writer.
		final FileIOChannel.ID channel = ioManager.createChannel(new File(ExternalBlockShuffleUtils.generateSpillPath(partitionDataRootPath, fileId)));
		ConcatPartitionedBufferSortedDataFile<T> writer = new ConcatPartitionedBufferSortedDataFile<T>(
			numberOfSubpartitions, channel, fileId, ioManager);
		channelDeleteRegistry.registerChannelToBeDelete(channel);
		channelDeleteRegistry.registerOpenChannel(writer.getWriteChannel());

		// Create file readers.
		final List<List<MemorySegment>> segments = Lists.partition(mergeReadMemory, mergeReadMemory.size() / toBeMerged.size());

		final Map<Integer, AsynchronousPartitionedStreamFileReaderDelegate> fileIndexToReaders = Maps.newHashMapWithExpectedSize(toBeMerged.size());
		final PartialOrderPriorityQueue<PartitionIndexStream> heap = new PartialOrderPriorityQueue<>(
			new PartitionIndexStreamComparator(),
			toBeMerged.size());

		List<FileIOChannel> channelAccessed = new ArrayList<>();
		for (int i = 0; i < toBeMerged.size(); ++i) {
			AsynchronousPartitionedStreamFileReaderDelegate readerDelegate =
				new AsynchronousPartitionedStreamFileReaderDelegate(
					ioManager, toBeMerged.get(i).getFile().getChannelID(), segments.get(i),
					toBeMerged.get(i).getFile().getPartitionIndexList());
			fileIndexToReaders.put(
				toBeMerged.get(i).getFileId(),
				readerDelegate);
			heap.add(new PartitionIndexStream(toBeMerged.get(i).getFile().getPartitionIndexList(), toBeMerged.get(i).getFileId()));
			channelAccessed.add(readerDelegate.getReader());
		}

		while (heap.size() > 0) {
			final PartitionIndexStream headStream = heap.peek();
			final PartitionIndex partitionIndex = headStream.getCurrentPartitionIndex();
			final int fileIndex = headStream.getFileIndex();

			if (!headStream.advance()) {
				heap.poll();
			} else {
				heap.adjustTop();
			}

			// now read the specific counts of buffers
			int readLength = 0;
			while (readLength < partitionIndex.getLength()) {
				Buffer buffer = fileIndexToReaders.get(fileIndex).getNextBufferBlocking();
				readLength += buffer.getSize();
				writer.writeBuffer(partitionIndex.getPartition(), buffer);
			}
			assert readLength == partitionIndex.getLength();
		}

		// Close the file reader for already merged files
		for (Map.Entry<Integer, AsynchronousPartitionedStreamFileReaderDelegate> entry : fileIndexToReaders.entrySet()) {
			entry.getValue().close();
		}

		writer.finishWriting();
		channelDeleteRegistry.unregisterOpenChannel(writer.getWriteChannel());

		clearMerged(channelAccessed, channelDeleteRegistry);

		// The fileId of a merged file is not equal to the id in spill file name. FileId is used to prevent merging
		// the same data multiple times in the same round.
		return new PartitionedFileAndFileId<>(writer, toBeMerged.get(0).getFileId());
	}

	private void clearMerged(List<FileIOChannel> needClear, ChannelDeleteRegistry<Tuple2<Integer, T>> channelDeleteRegistry) throws IOException {
		for (FileIOChannel channel : needClear) {
			channel.closeAndDelete();
			channelDeleteRegistry.unregisterOpenChannel(channel);
		}
		needClear.clear();
	}


	private static final class PartitionedFileAndFileId<T> {
		private final PartitionedSortedDataFile<T> file;
		private final int fileId;

		public PartitionedFileAndFileId(PartitionedSortedDataFile<T> file, int fileId) {
			this.file = file;
			this.fileId = fileId;
		}

		public PartitionedSortedDataFile<T> getFile() {
			return file;
		}

		public int getFileId() {
			return fileId;
		}
	}

	private static final class PartitionIndexStream {
		private final List<PartitionIndex> partitionIndices;
		private final int fileIndex;
		private int offset;

		public PartitionIndexStream(List<PartitionIndex> partitionIndices, int fileIndex) {
			this.partitionIndices = partitionIndices;
			this.fileIndex = fileIndex;
			this.offset = 0;
		}

		public PartitionIndex getCurrentPartitionIndex() {
			return partitionIndices.get(offset);
		}

		public int getFileIndex() {
			return fileIndex;
		}

		public boolean advance() {
			if (offset < partitionIndices.size() - 1) {
				offset++;
				return true;
			}

			return false;
		}

		@Override
		public String toString() {
			return "PartitionIndexStream{" +
				"partitionIndices=" + partitionIndices.size() +
				", fileIndex=" + fileIndex +
				", offset=" + offset +
				'}';
		}
	}

	private static final class PartitionIndexStreamComparator implements Comparator<PartitionIndexStream> {
		@Override
		public int compare(PartitionIndexStream first, PartitionIndexStream second) {
			int firstPartition = first.getCurrentPartitionIndex().getPartition();
			int secondPartition = second.getCurrentPartitionIndex().getPartition();
			if (firstPartition != secondPartition) {
				return firstPartition < secondPartition ? -1 : 1;
			}

			int firstFileIndex = first.getFileIndex();
			int secondFileIndex = second.getFileIndex();
			if (firstFileIndex != secondFileIndex) {
				return firstFileIndex < secondFileIndex ? -1 : 1;
			}

			long firstStart = first.getCurrentPartitionIndex().getStartOffset();
			long secondStart = second.getCurrentPartitionIndex().getStartOffset();
			return Long.compare(firstStart, secondStart);
		}
	}
}
