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

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The type Second of two input processor.
 */
class SecondOfTwoInputProcessor extends AbstractTwoInputProcessor {

	private Counter numRecordsIn;

	private final TwoInputStreamOperator operator;

	/**
	 * Instantiates a new Second of two input processor.
	 *
	 * @param streamStatusSubMaintainer the stream status sub maintainer
	 * @param operator                  the operator
	 * @param checkpointLock            the checkpoint lock
	 * @param taskMetricGroup           the task metric group
	 * @param minAllInputWatermarkGauge the min all input watermark gauge
	 * @param channelCount              the channel count
	 */
	public SecondOfTwoInputProcessor(
		StreamStatusSubMaintainer streamStatusSubMaintainer,
		TwoInputStreamOperator operator,
		Object checkpointLock,
		TaskMetricGroup taskMetricGroup,
		MinWatermarkGauge minAllInputWatermarkGauge,
		int channelCount) {

		super(streamStatusSubMaintainer, operator, checkpointLock, taskMetricGroup, minAllInputWatermarkGauge, channelCount);

		this.operator = checkNotNull(operator);

		numRecordsIn = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
	}

	@SuppressWarnings("unchecked")
	@Override
	void processRecord(StreamRecord streamRecord) throws Exception {
		numRecordsIn.inc();

		operator.setKeyContextElement2(streamRecord);
		operator.processRecord2(streamRecord);
	}

	@Override
	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker2(latencyMarker);
	}

	@Override
	public void endInput() throws Exception {
		operator.endInput2();
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		try {
			getInput2WatermarkGauge().setCurrentWatermark(watermark.getTimestamp());
			operator.processWatermark2(watermark);
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
		}
	}
}

