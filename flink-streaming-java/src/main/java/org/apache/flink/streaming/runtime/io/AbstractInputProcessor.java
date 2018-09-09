/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The type abstract input processor.
 */
abstract class AbstractInputProcessor implements InputProcessor, StatusWatermarkValve.ValveOutputHandler {

	private final StatusWatermarkValve statusWatermarkValve;

	private final StreamStatusSubMaintainer streamStatusSubMaintainer;

	private final Object checkpointLock;

	private final TaskMetricGroup taskMetricGroup;

	/**
	 * Instantiates a new abstract input processor.
	 *
	 * @param streamStatusSubMaintainer the stream status sub maintainer
	 * @param checkpointLock            the checkpoint lock
	 * @param taskMetricGroup           the task metric group
	 * @param channelCount              the channel count
	 */
	AbstractInputProcessor(
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		Object checkpointLock,
		TaskMetricGroup taskMetricGroup,
		int channelCount) {

		this.streamStatusSubMaintainer = checkNotNull(streamStatusSubMaintainer);
		this.checkpointLock = checkNotNull(checkpointLock);
		this.taskMetricGroup = checkNotNull(taskMetricGroup);
		this.statusWatermarkValve = new StatusWatermarkValve(channelCount, this);
	}

	@Override
	public void processElement(StreamElement streamElement, int channelIndex) throws Exception {
		synchronized (checkpointLock) {
			if (streamElement.isWatermark()) {
				statusWatermarkValve.inputWatermark(streamElement.asWatermark(), channelIndex);
			} else if (streamElement.isStreamStatus()) {
				statusWatermarkValve.inputStreamStatus(streamElement.asStreamStatus(), channelIndex);
			} else if (streamElement.isLatencyMarker()) {
				processLatencyMarker(streamElement.asLatencyMarker());
			} else {
				taskMetricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
				processRecord(streamElement.asRecord());
			}
		}
	}

	/**
	 * Process record.
	 *
	 * @param streamRecord the stream record
	 * @throws Exception the exception
	 */
	abstract void processRecord(StreamRecord streamRecord) throws Exception;

	/**
	 * Process latency marker.
	 *
	 * @param latencyMarker the latency marker
	 * @throws Exception the exception
	 */
	abstract void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

	@Override
	public void handleStreamStatus(StreamStatus streamStatus) {
		streamStatusSubMaintainer.updateStreamStatus(streamStatus);
	}

	@Override
	public void release() {
		streamStatusSubMaintainer.release();
	}
}


