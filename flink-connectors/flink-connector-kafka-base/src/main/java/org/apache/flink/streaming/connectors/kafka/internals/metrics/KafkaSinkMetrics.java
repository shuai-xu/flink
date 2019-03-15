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

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.connectors.metrics.LegacyMetricUtil;
import org.apache.flink.connectors.metrics.SinkMetrics;
import org.apache.flink.metrics.MetricGroup;

/**
 * The metrics for Kafka sinks. This is just standard sink metrics for now.
 */
public class KafkaSinkMetrics extends SinkMetrics {

	public static final String KAFKA_PRODUCER_GROUP = "KafkaProducer";

	// The following metrics are legacy metrics for Blink.
	private static final String LEGACY_SINK_IN_TPS_COUNTER = "inTps_counter";
	private static final String LEGACY_SINK_IN_TPS = "inTps";
	private static final String LEGACY_SINK_OUT_TPS_COUNTER = "outTps_counter";
	private static final String LEGACY_SINK_OUT_TPS = "outTps";
	private static final String LEGACY_SINK_OUT_BPS_COUNTER = "outBps_counter";
	private static final String LEGACY_SINK_OUT_BPS = "outBps";

	public KafkaSinkMetrics(MetricGroup metricGroup) {
		super(metricGroup.addGroup(KAFKA_PRODUCER_GROUP));

		// Report legacy metrics.
		this.metricGroup().counter(LEGACY_SINK_IN_TPS_COUNTER, LegacyMetricUtil.wrap(getCounter(NUM_RECORDS_OUT)));
		this.metricGroup().meter(LEGACY_SINK_IN_TPS, LegacyMetricUtil.wrap(getMeter(NUM_RECORDS_OUT_PER_SEC)));
		this.metricGroup().counter(LEGACY_SINK_OUT_TPS_COUNTER, LegacyMetricUtil.wrap(getCounter(NUM_RECORDS_OUT)));
		this.metricGroup().meter(LEGACY_SINK_OUT_TPS, LegacyMetricUtil.wrap(getMeter(NUM_RECORDS_OUT_PER_SEC)));
		this.metricGroup().counter(LEGACY_SINK_OUT_BPS_COUNTER, LegacyMetricUtil.wrap(getCounter(NUM_BYTES_OUT)));
		this.metricGroup().meter(LEGACY_SINK_OUT_BPS, LegacyMetricUtil.wrap(getMeter(NUM_BYTES_OUT_PER_SEC)));
	}
}
