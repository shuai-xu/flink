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

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusSubMaintainer;

import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for {@link OneInputProcessor}.
 */
public class OneInputProcessorTest {

	@Test
	public void testHandleWatermark() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final OneInputStreamOperator operator = mock(OneInputStreamOperator.class);
		final OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
		when(operator.getMetricGroup()).thenReturn(metricGroup);
		final OperatorIOMetricGroup operatorIOMetricGroup = mock(OperatorIOMetricGroup.class);
		when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
		final SimpleCounter operatorCounter = new SimpleCounter();
		when(operatorIOMetricGroup.getNumRecordsInCounter()).thenReturn(operatorCounter);
		final TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final OneInputProcessor processor = new OneInputProcessor(
			subMaintainer,
			operator,
			this,
			taskMetricGroup,
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamElement streamElement1 = new Watermark(123L);
		processor.processElement(streamElement1, 1);
		assertEquals(Long.MIN_VALUE, processor.getWatermarkGauge().getValue().longValue());

		final StreamElement streamElement2 = new Watermark(234L);
		processor.processElement(streamElement2, 0);
		assertEquals(123L, processor.getWatermarkGauge().getValue().longValue());

		verify(metricGroup, times(1)).gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, processor.getWatermarkGauge());
		verify(operator, times(1)).processWatermark(streamElement1.asWatermark());
		assertEquals(0, operatorCounter.getCount());
	}

	@Test
	public void testHandleStreamStatus() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final OneInputStreamOperator operator = mock(OneInputStreamOperator.class);
		final OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
		when(operator.getMetricGroup()).thenReturn(metricGroup);
		final OperatorIOMetricGroup operatorIOMetricGroup = mock(OperatorIOMetricGroup.class);
		when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
		final SimpleCounter operatorCounter = new SimpleCounter();
		when(operatorIOMetricGroup.getNumRecordsInCounter()).thenReturn(operatorCounter);
		final TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final OneInputProcessor processor = new OneInputProcessor(
			subMaintainer,
			operator,
			this,
			taskMetricGroup,
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamElement streamElement1 = new StreamStatus(StreamStatus.IDLE_STATUS);
		processor.processElement(streamElement1, 1);
		assertEquals(StreamStatus.ACTIVE, subMaintainer.getStreamStatus());
		assertEquals(StreamStatus.ACTIVE, parentMaintainer.getStreamStatus());

		final StreamElement streamElement2 = new StreamStatus(StreamStatus.IDLE_STATUS);
		processor.processElement(streamElement2, 0);
		assertEquals(StreamStatus.IDLE, subMaintainer.getStreamStatus());
		assertEquals(StreamStatus.IDLE, parentMaintainer.getStreamStatus());
	}

	@Test
	public void testProcessRecord() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final OneInputStreamOperator operator = mock(OneInputStreamOperator.class);
		final OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
		when(operator.getMetricGroup()).thenReturn(metricGroup);
		final OperatorIOMetricGroup operatorIOMetricGroup = mock(OperatorIOMetricGroup.class);
		when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
		final SimpleCounter operatorCounter = new SimpleCounter();
		when(operatorIOMetricGroup.getNumRecordsInCounter()).thenReturn(operatorCounter);

		final TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		final TaskIOMetricGroup ioMetricGroup = mock(TaskIOMetricGroup.class);
		when(taskMetricGroup.getIOMetricGroup()).thenReturn(ioMetricGroup);
		final SimpleCounter taskCounter = new SimpleCounter();
		when(ioMetricGroup.getNumRecordsInCounter()).thenReturn(taskCounter);
		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final OneInputProcessor processor = new OneInputProcessor(
			subMaintainer,
			operator,
			this,
			taskMetricGroup,
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamElement streamElement1 = new StreamRecord<>(123L);
		processor.processElement(streamElement1, 1);

		final StreamElement streamElement2 = new StreamRecord<>(234L);
		processor.processElement(streamElement2, 0);

		//noinspection unchecked
		verify(operator, times(2)).setKeyContextElement1(any(StreamRecord.class));
		//noinspection unchecked
		verify(operator, times(2)).processElement(any(StreamRecord.class));
		assertEquals(2, operatorCounter.getCount());
		assertEquals(2, taskCounter.getCount());
	}

	@Test
	public void testProcessLatencyMarker() throws Exception {
		final FakeStreamStatusMaintainer parentMaintainer = new FakeStreamStatusMaintainer();
		final BitSet bitSet = new BitSet();
		final StreamStatusSubMaintainer subMaintainer = new StreamStatusSubMaintainer(
			parentMaintainer,
			bitSet,
			0);
		final OneInputStreamOperator operator = mock(OneInputStreamOperator.class);
		final OperatorMetricGroup metricGroup = mock(OperatorMetricGroup.class);
		when(operator.getMetricGroup()).thenReturn(metricGroup);
		final OperatorIOMetricGroup operatorIOMetricGroup = mock(OperatorIOMetricGroup.class);
		when(metricGroup.getIOMetricGroup()).thenReturn(operatorIOMetricGroup);
		final SimpleCounter numRecordsIn = new SimpleCounter();
		when(operatorIOMetricGroup.getNumRecordsInCounter()).thenReturn(numRecordsIn);
		final TaskMetricGroup taskMetricGroup = mock(TaskMetricGroup.class);
		final MinWatermarkGauge minWatermarkGauge = new MinWatermarkGauge();
		final OneInputProcessor processor = new OneInputProcessor(
			subMaintainer,
			operator,
			this,
			taskMetricGroup,
			minWatermarkGauge,
			2);

		// There are 2 channels
		final StreamElement streamElement1 = new LatencyMarker(123L, new OperatorID(), 1);
		processor.processElement(streamElement1, 1);

		final StreamElement streamElement2 = new LatencyMarker(234L, new OperatorID(), 0);
		processor.processElement(streamElement2, 0);

		verify(operator, times(2)).processLatencyMarker(any(LatencyMarker.class));

		assertEquals(0, numRecordsIn.getCount());
	}

	class FakeStreamStatusMaintainer implements StreamStatusMaintainer {

		StreamStatus streamStatus = StreamStatus.ACTIVE;

		@Override
		public StreamStatus getStreamStatus() {
			return streamStatus;
		}

		@Override
		public void toggleStreamStatus(StreamStatus streamStatus) {
			this.streamStatus = streamStatus;
		}
	}
}
