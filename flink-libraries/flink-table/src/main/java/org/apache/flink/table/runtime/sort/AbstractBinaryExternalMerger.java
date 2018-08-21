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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.util.BinaryMergeIterator;
import org.apache.flink.table.util.ChannelWithMeta;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @param <Entry> Type of Entry to Merge sort.
 */
public abstract class AbstractBinaryExternalMerger<Entry> implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryExternalMerger.class);

	protected volatile boolean closed;

	protected final int pageSize;
	protected final IOManager ioManager;
	protected final int maxFanIn;
	protected final SpillChannelManager channelManager;

	public AbstractBinaryExternalMerger(
			IOManager ioManager,
			int pageSize,
			int maxFanIn,
			SpillChannelManager channelManager) {
		this.ioManager = ioManager;
		this.pageSize = pageSize;
		this.maxFanIn = maxFanIn;
		this.channelManager = channelManager;
	}

	@Override
	public void close() {
		this.closed = true;
	}

	/**
	 * Returns an iterator that iterates over the merged result from all given channels.
	 *
	 * @param channelIDs    The channels that are to be merged and returned.
	 * @param readBuffers The buffers to be used for reading. The list contains for each channel
	 *                      one list of input segments. The size of the <code>inputSegments</code>
	 *                      list must be equal to that of the <code>channelIDs</code> list.
	 * @return An iterator over the merged records of the input channels.
	 * @throws IOException Thrown, if the readers encounter an I/O problem.
	 */
	public BinaryMergeIterator<Entry> getMergingIterator(
			List<ChannelWithMeta> channelIDs,
			List<List<MemorySegment>> readBuffers,
			List<FileIOChannel> openChannels)
			throws IOException {
		// create one iterator per channel id
		if (LOG.isDebugEnabled()) {
			LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
		}

		final List<MutableObjectIterator<Entry>> iterators = new ArrayList<>(channelIDs.size() + 1);

		for (int i = 0; i < channelIDs.size(); i++) {
			final ChannelWithMeta channel = channelIDs.get(i);
			final List<MemorySegment> segsForChannel = readBuffers.get(i);

			// create a reader. if there are multiple segments for the reader, issue multiple together per I/O request
			final BlockChannelReader<MemorySegment> reader =
					ioManager.createBlockChannelReader(channel.getChannel());

			openChannels.add(reader);

			// wrap channel reader as a view, to get block spanning record deserialization
			final ChannelReaderInputView inView = new HeaderlessChannelReaderInputView(
							reader, segsForChannel, channel.getBlockCount(), channel.getNumBytesInLastBlock(), false);
			iterators.add(channelReaderInputViewIterator(inView));
		}

		return new BinaryMergeIterator<>(
				iterators, mergeReusedEntries(channelIDs.size()), mergeComparator());
	}

	/**
	 * Merges the given sorted runs to a smaller number of sorted runs.
	 *
	 * @param channelIDs     The IDs of the sorted runs that need to be merged.
	 * @param allReadBuffers memory segments for read channels.
	 * @param writeBuffers   The buffers to be used by the writers.
	 * @return A list of the IDs of the merged channels.
	 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
	 */
	public List<ChannelWithMeta> mergeChannelList(
			final List<ChannelWithMeta> channelIDs,
			final List<MemorySegment> allReadBuffers,
			final List<MemorySegment> writeBuffers) throws IOException {
		// A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1 rounds where every merge
		// is a full merge with maxFanIn input channels. A partial round includes merges with fewer than maxFanIn
		// inputs. It is most efficient to perform the partial round first.
		final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(maxFanIn)) - 1;

		final int numStart = channelIDs.size();
		final int numEnd = (int) Math.pow(maxFanIn, scale);

		final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1));

		final int numNotMerged = numEnd - numMerges;
		final int numToMerge = numStart - numNotMerged;

		// unmerged channel IDs are copied directly to the result list
		final List<ChannelWithMeta> mergedChannelIDs = new ArrayList<>(numEnd);
		mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

		final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

		// allocate the memory for the merging step
		final List<List<MemorySegment>> readBuffers = new ArrayList<>(channelsToMergePerStep);
		getSegmentsForReaders(readBuffers, allReadBuffers, channelsToMergePerStep);

		final List<ChannelWithMeta> channelsToMergeThisStep = new ArrayList<>(channelsToMergePerStep);
		int channelNum = numNotMerged;
		while (!closed && channelNum < channelIDs.size()) {
			channelsToMergeThisStep.clear();

			for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
				channelsToMergeThisStep.add(channelIDs.get(channelNum));
			}

			mergedChannelIDs.add(
					mergeChannels(channelsToMergeThisStep, readBuffers, writeBuffers));
		}

		return mergedChannelIDs;
	}


	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The
	 * merging process uses the given read and write buffers.
	 *
	 * @param channelIDs   The IDs of the runs' channels.
	 * @param readBuffers  The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID and number of blocks of the channel that describes the merged run.
	 */
	private ChannelWithMeta mergeChannels(
			List<ChannelWithMeta> channelIDs,
			List<List<MemorySegment>> readBuffers,
			List<MemorySegment> writeBuffers) throws IOException {
		// the list with the target iterators
		List<FileIOChannel> openChannels = new ArrayList<>(channelIDs.size());
		final BinaryMergeIterator<Entry> mergeIterator =
				getMergingIterator(channelIDs, readBuffers, openChannels);

		// create a new channel writer
		final FileIOChannel.ID mergedChannelID = ioManager.createChannel();
		channelManager.addChannel(mergedChannelID);
		BlockChannelWriter<MemorySegment> writer = null;

		int numBytesInLastBlock;
		int numBlocksWritten;
		try {
			writer = ioManager.createBlockChannelWriter(mergedChannelID);
			HeaderlessChannelWriterOutputView output =
					new HeaderlessChannelWriterOutputView(writer, writeBuffers, pageSize);
			// read the merged stream and write the data back
			writeMergingOutput(mergeIterator, output);
			numBytesInLastBlock = output.close();
			numBlocksWritten = output.getBlockCount();
		} catch (IOException e) {
			if (writer != null) {
				writer.closeAndDelete();
			}
			throw e;
		}

		// remove, close and delete channels
		for (FileIOChannel channel : openChannels) {
			channelManager.removeChannel(channel.getChannelID());
			try {
				channel.closeAndDelete();
			} catch (Throwable ignored) {
			}
		}

		return new ChannelWithMeta(mergedChannelID, numBlocksWritten, numBytesInLastBlock);
	}

	// -------------------------------------------------------------------------------------------

	/**
	 * @param inView
	 * @return entry iterator reading from inView.
	 */
	protected abstract MutableObjectIterator<Entry> channelReaderInputViewIterator(
			ChannelReaderInputView inView);

	/**
	 * @return merging comparator used in merging.
	 */
	protected abstract Comparator<Entry> mergeComparator();

	/**
	 * @return reused entry object used in merging.
	 */
	protected abstract List<Entry> mergeReusedEntries(int size);

	/**
	 * read the merged stream and write the data back.
	 */
	protected abstract void writeMergingOutput(
			MutableObjectIterator<Entry> mergeIterator,
			HeaderlessChannelWriterOutputView output) throws IOException;

	// -------------------------------------------------------------------------------------------

	/**
	 * Divides the given collection of memory buffers among {@code numChannels} sublists.
	 *
	 * @param target      The list into which the lists with buffers for the channels are put.
	 * @param memory      A list containing the memory buffers to be distributed. The buffers
	 *                    are not removed from this list.
	 * @param numChannels The number of channels for which to allocate buffers. Must not be
	 *                    zero.
	 */
	public static void getSegmentsForReaders(
			List<List<MemorySegment>> target,
			List<MemorySegment> memory,
			int numChannels) {
		// determine the memory to use per channel and the number of buffers
		final int numBuffers = memory.size();
		final int buffersPerChannelLowerBound = numBuffers / numChannels;
		final int numChannelsWithOneMore = numBuffers % numChannels;

		final Iterator<MemorySegment> segments = memory.iterator();

		// collect memory for the channels that get one segment more
		for (int i = 0; i < numChannelsWithOneMore; i++) {
			final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound + 1);
			target.add(segs);
			for (int k = buffersPerChannelLowerBound; k >= 0; k--) {
				segs.add(segments.next());
			}
		}

		// collect memory for the remaining channels
		for (int i = numChannelsWithOneMore; i < numChannels; i++) {
			final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound);
			target.add(segs);
			for (int k = buffersPerChannelLowerBound; k > 0; k--) {
				segs.add(segments.next());
			}
		}
	}
}
