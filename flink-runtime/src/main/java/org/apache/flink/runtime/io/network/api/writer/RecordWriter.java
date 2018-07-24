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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	protected final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final int numChannels;

	private final RecordSerializer<T> serializer;

	private final Optional<BufferBuilder>[] bufferBuilders;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private Counter numBytesOut = new SimpleCounter();

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, false);
	}

	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways) {
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;
		this.numChannels = writer.getNumberOfSubpartitions();

		this.serializer = new SpanningRecordSerializer<T>();

		this.bufferBuilders = new Optional[numChannels];
		for (int i = 0; i < numChannels; i++) {
			bufferBuilders[i] = Optional.empty();
		}
	}

	public void emit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			copyToTarget(targetChannel);
		}

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		serializer.prune();
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			copyToTarget(targetChannel);
		}

		serializer.prune();
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		copyToTarget(rng.nextInt(numChannels));

		serializer.prune();
	}

	private void copyToTarget(int targetChannel) throws IOException, InterruptedException {
		// We should reset the initial position of the intermediate serialization data buffer before
		// copying, so the serialization results can be copied to many different target buffers.
		serializer.reset();

		BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
		SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
		while (result.isFullBuffer()) {
			tryFinishCurrentBufferBuilder(targetChannel);

			// If this was a full record, we are done. Not breaking
			// out of the loop at this point will lead to another
			// buffer request before breaking out (that would not be
			// a problem per se, but it can lead to stalls in the pipeline).
			if (result.isFullRecord()) {
				break;
			}

			bufferBuilder = requestNewBufferBuilder(targetChannel);
			result = serializer.copyToBufferBuilder(bufferBuilder);
		}

		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				tryFinishCurrentBufferBuilder(targetChannel);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			closeBufferBuilder(targetChannel);
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished if exists.
	 */
	private void tryFinishCurrentBufferBuilder(int targetChannel) {
		if (!bufferBuilders[targetChannel].isPresent()) {
			return;
		}

		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();
		numBytesOut.inc(bufferBuilder.finish());
	}

	/**
	 * The {@link BufferBuilder} may already exist if not filled up last time, otherwise we need
	 * request a new one for this target channel.
	 */
	@Nonnull
	private BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		if (bufferBuilders[targetChannel].isPresent()) {
			return bufferBuilders[targetChannel].get();
		} else {
			return requestNewBufferBuilder(targetChannel);
		}
	}

	@Nonnull
	private BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(!bufferBuilders[targetChannel].isPresent());

		BufferBuilder bufferBuilder = targetPartition.getBufferProvider().requestBufferBuilderBlocking();
		bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);

		return bufferBuilder;
	}

	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}
}
