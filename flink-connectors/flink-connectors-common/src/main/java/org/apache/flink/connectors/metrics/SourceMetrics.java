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

package org.apache.flink.connectors.metrics;

import org.apache.flink.metrics.AbstractMetrics;
import org.apache.flink.metrics.BlackHoleMetric;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricDef;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricSpec;

import java.util.Collections;
import java.util.Map;

/**
 * A class hosting common connector metrics.
 */
public abstract class SourceMetrics extends AbstractMetrics {

	public static final String NUM_BYTES_IN = "numBytesIn";
	private static final String NUM_BYTES_IN_DOC = "The total number of input bytes since the source started.";

	public static final String NUM_BYTES_IN_PER_SEC = "numBytesInPerSec";
	private static final String NUM_BYTES_IN_PER_SEC_DOC = "The input bytes per second.";

	public static final String NUM_RECORDS_IN = "numRecordsIn";
	private static final String NUM_RECORDS_IN_DOC = "The total number of input records since the source started.";

	public static final String NUM_RECORDS_IN_PER_SEC = "numRecordsInPerSec";
	private static final String NUM_RECORDS_IN_PER_SEC_DOC = "The input records per second.";

	public static final String NUM_RECORDS_IN_ERRORS = "numRecordsInErrors";
	private static final String NUM_RECORDS_IN_ERRORS_DOC = "The number of errors received in consuming the records.";

	public static final String RECORD_SIZE = "recordSize";
	private static final String RECORD_SIZE_DOC = "The size of the record.";

	public static final String CURRENT_FETCH_LATENCY = "currentFetchLatency";
	private static final String CURRENT_FETCH_LATENCY_DOC =
			"The latency occurred before Flink fetched the record.\n" +
			"This metric is different from fetchLatency in that it is an instantaneous value recorded for the " +
			"last processed record.\n" +
			"This metric is provided because latency histogram could be expensive. The instantaneous latency " +
			"value is usually a good enough indication of the latency.\n" +
			"fetchLatency = FetchTime - EventTime";

	public static final String CURRENT_LATENCY = "currentLatency";
	private static final String CURRENT_LATENCY_DOC =
			"The latency occurred before the record is emitted by the source connector.\n" +
			"This metric is different from latency in that it is an instantaneous value recorded for the last " +
			"processed record.\n" +
			"This metric is provided because latency histogram could be expensive. The instantaneous latency " +
			"value is usually a good enough indication of the latency.\n" +
			"latency = EmitTime - EventTime";

	public static final String FETCH_LATENCY = "fetchLatency";
	private static final String FETCH_LATENCY_DOC = "The latency occurred before Flink fetched the record. "
		+ "fetchLatency = FetchTime - EventTime.";

	public static final String LATENCY = "latency";
	public static final String LATENCY_DOC = "The latency occurred before the record is emitted by the source "
		+ "connector. latency = EmitTime - EventTime.";

	public static final String IDLE_TIME = "idleTime";
	public static final String IDLE_TIME_DOC = "The time in milliseconds that the source has not processed any record. "
		+ "idleTime = CurrentTime - LastRecordProcessTime.";

	// The common metric def for all sources. Histograms are disabled by default.
	private static final MetricDef METRIC_DEF = new MetricDef()
		.define(
			NUM_BYTES_IN,
			NUM_BYTES_IN_DOC,
			MetricSpec.counter())
		.define(
			NUM_BYTES_IN_PER_SEC,
			NUM_BYTES_IN_PER_SEC_DOC,
			MetricSpec.meter(NUM_BYTES_IN))
		.define(
			NUM_RECORDS_IN,
			NUM_RECORDS_IN_DOC,
			MetricSpec.counter())
		.define(
			NUM_RECORDS_IN_PER_SEC,
			NUM_RECORDS_IN_PER_SEC_DOC,
			MetricSpec.meter(NUM_RECORDS_IN))
		.define(
			NUM_RECORDS_IN_ERRORS,
			NUM_RECORDS_IN_ERRORS_DOC,
			MetricSpec.counter())
		.define(
			RECORD_SIZE,
			RECORD_SIZE_DOC,
			MetricSpec.histogram(),
			false)
		.define(
			CURRENT_FETCH_LATENCY,
			CURRENT_FETCH_LATENCY_DOC,
			MetricSpec.gauge())
		.define(
			CURRENT_LATENCY,
			CURRENT_LATENCY_DOC,
			MetricSpec.gauge())
		.define(
			FETCH_LATENCY,
			FETCH_LATENCY_DOC,
			MetricSpec.histogram(),
			false)
		.define(
			LATENCY,
			LATENCY_DOC,
			MetricSpec.histogram(),
			false)
		.define(
			IDLE_TIME,
			IDLE_TIME_DOC,
			MetricSpec.gauge());

	/** A micro-optimization to keep references to some of the metrics to avoid frequent map lookup. */
	// Only need to update the bytes-in-rate and records-in-rate and the total counter would be updated.
	public final Meter numBytesInPerSec;
	public final Meter numRecordsInPerSec;
	public final Histogram recordSize;
	public final Histogram fetchLatency;
	public final Histogram latency;

	private volatile long lastRecordProcessTime = 0L;

	public SourceMetrics(MetricGroup metricGroup) {
		this(metricGroup, new MetricDef());
	}

	protected SourceMetrics(MetricGroup metricGroup, MetricDef additionalDef) {
		this(metricGroup, additionalDef, Collections.emptyMap());
	}

	protected SourceMetrics(MetricGroup metricGroup, MetricDef additionalDef, Map<String, Boolean> metricSwitch) {
		super(metricGroup, METRIC_DEF.combine(additionalDef), metricSwitch);

		maybeSetGauge(IDLE_TIME, (Gauge<Long>) () -> System.currentTimeMillis() - lastRecordProcessTime);

		numBytesInPerSec = getIfDefined(NUM_BYTES_IN_PER_SEC);
		numRecordsInPerSec = getIfDefined(NUM_RECORDS_IN_PER_SEC);
		recordSize = getIfDefined(RECORD_SIZE);
		fetchLatency = getIfDefined(FETCH_LATENCY);
		latency = getIfDefined(LATENCY);
	}

	/**
	 * Update the time when the last record was processed.
	 *
	 * @param time the time when the last record was processed.
	 */
	public void updateLastRecordProcessTime(long time) {
		lastRecordProcessTime = time;
	}

	private void maybeSetGauge(String name, Gauge gauge) {
		if (allMetricNames().contains(name)) {
			setGauge(name, gauge);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T getIfDefined(String name) {
		return (T) (allMetricNames().contains(name) ? get(name) : BlackHoleMetric.instance());
	}
}
