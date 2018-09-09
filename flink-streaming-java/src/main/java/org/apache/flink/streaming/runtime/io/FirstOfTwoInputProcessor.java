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
 * The type First of two input processor.
 */
class FirstOfTwoInputProcessor extends AbstractTwoInputProcessor {

	private Counter numRecordsIn;

	private final TwoInputStreamOperator operator;

	/**
	 * Instantiates a new First of two input processor.
	 *
	 * @param streamStatusSubMaintainer the stream status sub maintainer
	 * @param operator                  the operator
	 * @param checkpointLock            the checkpoint lock
	 * @param taskMetricGroup           the task metric group
	 * @param minAllInputWatermarkGauge the min all input watermark gauge
	 * @param channelCount              the channel count
	 */
	public FirstOfTwoInputProcessor(
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

		operator.setKeyContextElement1(streamRecord);
		operator.processRecord1(streamRecord);
	}

	@Override
	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		operator.processLatencyMarker1(latencyMarker);
	}

	@Override
	public void endInput() throws Exception {
		operator.endInput1();
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		try {
			getInput1WatermarkGauge().setCurrentWatermark(watermark.getTimestamp());
			operator.processWatermark1(watermark);
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
		}
	}
}

