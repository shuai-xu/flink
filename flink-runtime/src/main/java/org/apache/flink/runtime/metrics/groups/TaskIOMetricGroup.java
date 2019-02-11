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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.InternalResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.runtime.taskmanager.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {

	private final Counter numBytesOut;
	private final Counter numBuffersOut;
	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;
	private final Counter numRecordsSent;
	private final Counter numRecordsReceived;
	private final SumCounter numRecordsIn;
	private final SumCounter numRecordsOut;

	private final SumAndCount nsWaitBufferTime;

	private final Meter numBytesInRateLocal;
	private final Meter numBytesInRateRemote;
	private final Meter numBytesOutRate;
	private final Meter numRecordsInRate;
	private final Meter numRecordsOutRate;

	public TaskIOMetricGroup(TaskMetricGroup parent) {
		super(parent);

		this.numBytesOut = counter(MetricNames.IO_NUM_BYTES_OUT);
		this.numBuffersOut = counter(MetricNames.IO_NUM_BUFFERS_OUT);
		this.numBytesInLocal = counter(MetricNames.IO_NUM_BYTES_IN_LOCAL);
		this.numBytesInRemote = counter(MetricNames.IO_NUM_BYTES_IN_REMOTE);
		this.numBytesOutRate = meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut, 60));
		this.numBytesInRateLocal = meter(MetricNames.IO_NUM_BYTES_IN_LOCAL_RATE, new MeterView(numBytesInLocal, 60));
		this.numBytesInRateRemote = meter(MetricNames.IO_NUM_BYTES_IN_REMOTE_RATE, new MeterView(numBytesInRemote, 60));
		this.numRecordsIn = counter(MetricNames.IO_NUM_RECORDS_IN, new SumCounter());
		this.numRecordsOut = counter(MetricNames.IO_NUM_RECORDS_OUT, new SumCounter());
		this.numRecordsInRate = meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn, 60));
		this.numRecordsOutRate = meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut, 60));
		this.nsWaitBufferTime = new SumAndCount(MetricNames.IO_WAIT_BUFFER_TIME, parent);
		this.numRecordsSent = counter(MetricNames.IO_NUM_RECORDS_SENT);
		this.numRecordsReceived = counter(MetricNames.IO_NUM_RECORDS_RECEIVED);
	}

	public IOMetrics createSnapshot() {
		return new IOMetrics(numRecordsInRate, numRecordsOutRate, numBytesInRateLocal, numBytesInRateRemote, numBytesOutRate);
	}

	// ============================================================================================
	// Getters
	// ============================================================================================
	public Counter getNumBytesOutCounter() {
		return numBytesOut;
	}

	public Counter getNumBuffersOutCounter() {
		return numBuffersOut;
	}

	public Counter getNumBytesInLocalCounter() {
		return numBytesInLocal;
	}

	public Counter getNumBytesInRemoteCounter() {
		return numBytesInRemote;
	}

	public Counter getNumRecordsInCounter() {
		return numRecordsIn;
	}

	public Counter getNumRecordsOutCounter() {
		return numRecordsOut;
	}

	public Meter getNumBytesInLocalRateMeter() {
		return numBytesInRateLocal;
	}

	public Meter getNumBytesInRemoteRateMeter() {
		return numBytesInRateRemote;
	}

	public Meter getNumBytesOutRateMeter() {
		return numBytesOutRate;
	}

	public Counter getNumRecordsSent() {
		return numRecordsSent;
	}
	// ============================================================================================
	// Buffer metrics
	// ============================================================================================

	/**
	 * Initialize Buffer Metrics for a task.
	 */
	public void initializeBufferMetrics(Task task) {
		final MetricGroup buffers = addGroup(MetricNames.BUFFERS);
		buffers.gauge(MetricNames.BUFFERS_INPUT_QUEUE_LENGTH, new InputBuffersGauge(task));
		buffers.gauge(MetricNames.BUFFERS_OUT_QUEUE_LENGTH, new OutputBuffersGauge(task));
		buffers.gauge(MetricNames.BUFFERS_IN_POOL_USAGE, new InputBufferPoolUsageGauge(task));
		buffers.gauge(MetricNames.BUFFERS_OUT_POOL_USAGE, new OutputBufferPoolUsageGauge(task));
	}

	public Counter getNumRecordsReceived() {
		return numRecordsReceived;
	}

	/**
	 * Gauge measuring the number of queued input buffers of a task.
	 */
	private static final class InputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public InputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for (SingleInputGate inputGate : task.getAllInputGates()) {
				totalBuffers += inputGate.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Gauge measuring the number of queued output buffers of a task.
	 */
	private static final class OutputBuffersGauge implements Gauge<Integer> {

		private final Task task;

		public OutputBuffersGauge(Task task) {
			this.task = task;
		}

		@Override
		public Integer getValue() {
			int totalBuffers = 0;

			for (InternalResultPartition producedPartition : task.getInternalPartitions()) {
				totalBuffers += producedPartition.getNumberOfQueuedBuffers();
			}

			return totalBuffers;
		}
	}

	/**
	 * Gauge measuring the input buffer pool usage gauge of a task.
	 */
	private static final class InputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public InputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			float maxUsage = 0;

			for (SingleInputGate inputGate : task.getAllInputGates()) {
				BufferPool bufferPool = inputGate.getBufferPool();

				int usedBuffers = bufferPool.bestEffortGetNumOfUsedBuffers();
				int bufferPoolSize = bufferPool.getNumBuffers();

				if (bufferPoolSize != 0) {
					float currentPoolUsage = ((float) usedBuffers) / bufferPoolSize;

					if (currentPoolUsage > maxUsage) {
						maxUsage = currentPoolUsage;
					}
				}
			}

			return maxUsage;
		}
	}

	/**
	 * Gauge measuring the output buffer pool usage gauge of a task.
	 */
	private static final class OutputBufferPoolUsageGauge implements Gauge<Float> {

		private final Task task;

		public OutputBufferPoolUsageGauge(Task task) {
			this.task = task;
		}

		@Override
		public Float getValue() {
			float maxUsage = 0;

			for (InternalResultPartition internalResultPartition : task.getInternalPartitions()) {
				BufferPool bufferPool = internalResultPartition.getBufferPool();

				int usedBuffers = bufferPool.bestEffortGetNumOfUsedBuffers();
				int bufferPoolSize = bufferPool.getNumBuffers();

				if (bufferPoolSize != 0) {
					float currentPoolUsage = ((float) usedBuffers) / bufferPoolSize;
					if (currentPoolUsage > maxUsage) {
						maxUsage = currentPoolUsage;
					}
				}
			}

			return maxUsage;
		}
	}

	// ============================================================================================
	// Metric Reuse
	// ============================================================================================
	public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
		this.numRecordsIn.addCounter(numRecordsInCounter);
	}

	public void reuseRecordsOutputCounter(Counter numRecordsOutCounter) {
		this.numRecordsOut.addCounter(numRecordsOutCounter);
	}

	public SumAndCount getNsWaitBufferTime() {
		return nsWaitBufferTime;
	}

	/**
	 * A {@link SimpleCounter} that can contain other {@link Counter}s. A call to {@link SumCounter#getCount()} returns
	 * the sum of this counters and all contained counters.
	 */
	private static class SumCounter extends SimpleCounter {
		private final List<Counter> internalCounters = new ArrayList<>();

		SumCounter() {
		}

		public void addCounter(Counter toAdd) {
			internalCounters.add(toAdd);
		}

		@Override
		public long getCount() {
			long sum = super.getCount();
			for (Counter counter : internalCounters) {
				sum += counter.getCount();
			}
			return sum;
		}
	}
}
