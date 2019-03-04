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

import org.apache.flink.metrics.AbstractMetrics;
import org.apache.flink.metrics.MetricDef;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricSpec;

/**
 * The metrics defined for each Kafka source topic and partitions.
 */
public class KafkaTopicPartitionMetrics extends AbstractMetrics {

	public static final String OFFSETS_BY_TOPIC_METRICS_GROUP = "topic";
	public static final String OFFSETS_BY_PARTITION_METRICS_GROUP = "partition";

	public static final String CURRENT_OFFSETS_METRICS_GAUGE = "currentOffsets";
	private static final String CURRENT_OFFSET_METRICS_GAUGE_DOC = "The current offset of this topic partition.";

	public static final String COMMITTED_OFFSETS_METRICS_GAUGE = "committedOffsets";
	private static final String COMMITTED_OFFSETS_METRICS_GAUGE_DOC = "The committed offset of this topic partition";

	public static final String LEGACY_CURRENT_OFFSETS_METRICS_GROUP = "current-offsets";
	public static final String LEGACY_COMMITTED_OFFSETS_METRICS_GROUP = "committed-offsets";

	private static final MetricDef METRIC_DEF = new MetricDef()
		.define(
			CURRENT_OFFSETS_METRICS_GAUGE,
			CURRENT_OFFSET_METRICS_GAUGE_DOC,
			MetricSpec.gauge())
		.define(
			COMMITTED_OFFSETS_METRICS_GAUGE,
			COMMITTED_OFFSETS_METRICS_GAUGE_DOC,
			MetricSpec.gauge());

	public KafkaTopicPartitionMetrics(MetricGroup metricGroup, String topic, int partition) {
		super(
			metricGroup
				.addGroup(OFFSETS_BY_TOPIC_METRICS_GROUP, topic)
				.addGroup(OFFSETS_BY_PARTITION_METRICS_GROUP, Integer.toString(partition)),
			METRIC_DEF);
	}
}
