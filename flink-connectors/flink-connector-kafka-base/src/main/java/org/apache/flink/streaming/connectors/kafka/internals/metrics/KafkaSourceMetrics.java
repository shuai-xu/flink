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
import org.apache.flink.connectors.metrics.SourceMetrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricDef;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricSpec;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.metrics.MetricNames.IO_NUM_TPS;

/**
 * A class that holds all Kafka Source Metrics.
 */
public class KafkaSourceMetrics extends SourceMetrics {

	public static final String KAFKA_CONSUMER_METRICS_GROUP = "KafkaConsumer";

	public static final String COMMITS_SUCCEEDED_METRICS_COUNTER = "commitsSucceeded";
	private static final String COMMITS_SUCCEEDED_METRICS_COUNTER_DOC = "The total number of successful offset commits.";

	public static final String COMMITS_FAILED_METRICS_COUNTER = "commitsFailed";
	private static final String COMMITS_FAILED_METRICS_COUNTER_DOC = "The total number of failed offset commits.";

	// The following metrics are legacy metrics in Blink
	private static final String LEGACY_TPS_COUNTER = IO_NUM_TPS + "_counter";
	private static final String LEGACY_TPS = IO_NUM_TPS;
	private static final String LEGACY_RPS = "parserTps";
	private static final String LEGACY_BPS_COUNTER = "inBps_counter";
	private static final String LEGACY_BPS = "inBps";
	private static final String LEGACY_DELAY = "delay";
	private static final String LEGACY_FETCHED_DELAY = "fetched_delay";
	private static final String LEGACY_NO_DATA_DELAY = "no_data_delay";

	private static final MetricDef METRIC_DEF = new MetricDef()
		.define(
			COMMITS_SUCCEEDED_METRICS_COUNTER,
			COMMITS_SUCCEEDED_METRICS_COUNTER_DOC,
			MetricSpec.counter())
		.define(
			COMMITS_FAILED_METRICS_COUNTER,
			COMMITS_FAILED_METRICS_COUNTER_DOC,
			MetricSpec.counter());

	// initialize commit metrics and default offset callback method
	public final Counter successfulCommits;
	public final Counter failedCommits;

	private final Map<TopicPartition, Long> lastProcessedTime = new ConcurrentHashMap<>();
	private final Map<TopicPartition, Long> lastFetchedTime = new ConcurrentHashMap<>();

	public KafkaSourceMetrics(MetricGroup metricGroup) {
		super(metricGroup.addGroup(KAFKA_CONSUMER_METRICS_GROUP), METRIC_DEF);

		successfulCommits = get(COMMITS_SUCCEEDED_METRICS_COUNTER);
		failedCommits = get(COMMITS_FAILED_METRICS_COUNTER);

		setGauge(CURRENT_LATENCY,
				() -> lastProcessedTime.isEmpty() ? -1L : System.currentTimeMillis() - Collections.min(lastProcessedTime.values()));
		setGauge(CURRENT_FETCH_LATENCY,
				() -> lastFetchedTime.isEmpty() ? -1L : System.currentTimeMillis() - Collections.min(lastFetchedTime.values()));

		// Report legacy metrics for Blink.
		this.metricGroup().counter(LEGACY_TPS_COUNTER, LegacyMetricUtil.wrap(getCounter(NUM_RECORDS_IN)));
		this.metricGroup().meter(LEGACY_TPS, LegacyMetricUtil.wrap(getMeter(NUM_RECORDS_IN_PER_SEC)));
		this.metricGroup().meter(LEGACY_RPS, LegacyMetricUtil.wrap(getMeter(NUM_RECORDS_IN_PER_SEC)));
		this.metricGroup().counter(LEGACY_BPS_COUNTER, LegacyMetricUtil.wrap(getCounter(NUM_BYTES_IN)));
		this.metricGroup().meter(LEGACY_BPS, LegacyMetricUtil.wrap(getMeter(NUM_BYTES_IN_PER_SEC)));
		this.metricGroup().gauge(LEGACY_DELAY, LegacyMetricUtil.wrap(getGauge(CURRENT_LATENCY)));
		this.metricGroup().gauge(LEGACY_FETCHED_DELAY, LegacyMetricUtil.wrap(getGauge(CURRENT_FETCH_LATENCY)));
		this.metricGroup().gauge(LEGACY_NO_DATA_DELAY, LegacyMetricUtil.wrap(getGauge(IDLE_TIME)));
	}

	public void updateLastProcessTime(TopicPartition tp, long time) {
		lastProcessedTime.put(tp, time);
	}

	public void updateLastFetchTime(TopicPartition tp, long time) {
		lastFetchedTime.put(tp, time);
	}

	public void updatePartitions(Collection<TopicPartition> partitions) {
		lastFetchedTime.entrySet().removeIf(entry -> !partitions.contains(entry.getKey()));
		lastProcessedTime.entrySet().removeIf(entry -> !partitions.contains(entry.getKey()));
	}
}
