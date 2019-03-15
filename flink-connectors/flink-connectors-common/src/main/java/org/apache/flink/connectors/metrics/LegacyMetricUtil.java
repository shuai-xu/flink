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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;

/**
 * Util class for legacy metrics. Should be removed once all the metric name migration finishes.
 */
public class LegacyMetricUtil {

	@SuppressWarnings("unchecked")
	public static <T> T wrap(Metric metric) {
		if (metric instanceof Counter) {
			return (T) new CounterWrapper((Counter) metric);
		} else if (metric instanceof Meter) {
			return (T) new MeterWrapper((Meter) metric);
		} else if (metric instanceof Gauge) {
			return (T) new GaugeWrapper((Gauge<?>) metric);
		} else if (metric instanceof Histogram) {
			return (T) new HistogramWrapper((Histogram) metric);
		} else {
			throw new IllegalArgumentException("Metric type " + metric.getClass().getName() + " cannot be wrapped");
		}
	}

	// ----------------------- wrapper classes ---------------------------
	private static final class CounterWrapper implements Counter {
		private final Counter counter;

		private CounterWrapper(Counter counter) {
			this.counter = counter;
		}

		@Override
		public void inc() {
			counter.inc();
		}

		@Override
		public void inc(long n) {
			counter.inc(n);
		}

		@Override
		public void dec() {
			counter.dec();
		}

		@Override
		public void dec(long n) {
			counter.dec(n);
		}

		@Override
		public long getCount() {
			return counter.getCount();
		}
	}

	private static final class GaugeWrapper<T> implements Gauge<T> {
		private final Gauge<T> gauge;

		private GaugeWrapper(Gauge<T> gauge) {
			this.gauge = gauge;
		}

		@Override
		public T getValue() {
			return gauge.getValue();
		}
	}

	private static final class MeterWrapper implements Meter {
		private final Meter meter;

		private MeterWrapper(Meter meter) {
			this.meter = meter;
		}

		@Override
		public void markEvent() {
			meter.markEvent();
		}

		@Override
		public void markEvent(long n) {
			meter.markEvent(n);
		}

		@Override
		public double getRate() {
			return meter.getRate();
		}

		@Override
		public long getCount() {
			return meter.getCount();
		}
	}

	private static final class HistogramWrapper implements Histogram {
		private final Histogram histogram;

		private HistogramWrapper(Histogram histogram) {
			this.histogram = histogram;
		}

		@Override
		public void update(long value) {
			histogram.update(value);
		}

		@Override
		public long getCount() {
			return histogram.getCount();
		}

		@Override
		public HistogramStatistics getStatistics() {
			return histogram.getStatistics();
		}
	}
}
