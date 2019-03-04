/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.connectors.metrics;

import org.apache.flink.metrics.AbstractMetrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricDef;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricSpec;

/**
 * Abstract class for the SinkMetrics.
 */
public abstract class SinkMetrics extends AbstractMetrics {
	public static final String NUM_BYTES_OUT = "numBytesOut";
	private static final String NUM_BYTES_OUT_DOC = "The total number of output bytes since the source started.";

	public static final String NUM_BYTES_OUT_PER_SEC = "numBytesOutPerSec";
	private static final String NUM_BYTES_OUT_PER_SEC_DOC = "The output bytes per second.";

	public static final String NUM_RECORDS_OUT = "numRecordsOut";
	private static final String NUM_RECORDS_OUT_DOC = "The total number of output records since the source started.";

	public static final String NUM_RECORDS_OUT_PER_SEC = "numRecordsOutPerSec";
	private static final String NUM_RECORDS_OUT_PER_SEC_DOC = "The output records per second.";

	public static final String NUM_RECORDS_OUT_ERRORS = "numRecordsOutErrors";
	private static final String NUM_RECORDS_OUT_ERRORS_DOC = "The total number of records failed to send.";

	public static final String RECORD_SIZE = "recordSize";
	private static final String RECORD_SIZE_DOC = "The size of a record.";

	public static final String SEND_TIME = "sendTime";
	private static final String SEND_TIME_DOC = "The time it takes to send a record.";

	// Disable histograms by default for performance consideration.
	private static final MetricDef METRIC_DEF = new MetricDef()
		.define(
			NUM_BYTES_OUT,
			NUM_BYTES_OUT_DOC,
			MetricSpec.counter())
		.define(
			NUM_BYTES_OUT_PER_SEC,
			NUM_BYTES_OUT_PER_SEC_DOC,
			MetricSpec.meter(NUM_BYTES_OUT))
		.define(
			NUM_RECORDS_OUT,
			NUM_RECORDS_OUT_DOC,
			MetricSpec.counter())
		.define(
			NUM_RECORDS_OUT_PER_SEC,
			NUM_RECORDS_OUT_PER_SEC_DOC,
			MetricSpec.meter(NUM_RECORDS_OUT))
		.define(
			NUM_RECORDS_OUT_ERRORS,
			NUM_RECORDS_OUT_ERRORS_DOC,
			MetricSpec.counter())
		.define(
			RECORD_SIZE,
			RECORD_SIZE_DOC,
			MetricSpec.histogram(),
			false)
		.define(
			SEND_TIME,
			SEND_TIME_DOC,
			MetricSpec.histogram(),
			false);

	/** A micro-optimization: hold the metrics to avoid frequent metric object lookup. */
	public final Meter numBytesOutPerSec;
	public final Meter numRecordsOutPerSec;
	public final Counter numRecordsOutErrors;
	public final Histogram recordSize;
	public final Histogram sendTime;

	/**
	 * Construct the metrics based on the given metric group.
	 *
	 * @param metricGroup the metric group to register the metrics.
	 */
	public SinkMetrics(MetricGroup metricGroup) {
		this(metricGroup, new MetricDef());
	}

	/**
	 * Construct the sink metrics based on the given metric group and additional metric definitions.
	 *
	 * @param metricGroup the metric group to register the metrics.
	 * @param additionalMetrics the metric definitions in addition to the standard sink metrics.
	 */
	protected SinkMetrics(MetricGroup metricGroup, MetricDef additionalMetrics) {
		super(metricGroup, METRIC_DEF.combine(additionalMetrics));
		this.numBytesOutPerSec = get(NUM_BYTES_OUT_PER_SEC);
		this.numRecordsOutPerSec = get(NUM_RECORDS_OUT_PER_SEC);
		this.numRecordsOutErrors = get(NUM_RECORDS_OUT_ERRORS);
		this.recordSize = get(RECORD_SIZE);
		this.sendTime = get(SEND_TIME);
	}
}
