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

package org.apache.flink.metrics;

/**
 * A black hole metric to avoid null check when connector report metrics. This metrics
 * does nothing when record values and does not expect any reading either.
 */
public class BlackHoleMetric implements Histogram, Meter, Counter, Gauge {

	private static final BlackHoleMetric BLACK_HOLE_METRIC = new BlackHoleMetric();

	// Private constructor for singleton class.
	private BlackHoleMetric() {

	}

	public static BlackHoleMetric instance() {
		return BLACK_HOLE_METRIC;
	}

	@Override
	public void inc() {

	}

	@Override
	public void inc(long n) {

	}

	@Override
	public void dec() {

	}

	@Override
	public void dec(long n) {

	}

	@Override
	public void update(long value) {

	}

	@Override
	public void markEvent() {

	}

	@Override
	public void markEvent(long n) {

	}

	@Override
	public double getRate() {
		throw new IllegalStateException("The black hole metric does not expect any reading.");
	}

	@Override
	public long getCount() {
		throw new IllegalStateException("The black hole metric does not expect any reading.");
	}

	@Override
	public HistogramStatistics getStatistics() {
		throw new IllegalStateException("The black hole metric does not expect any reading.");
	}

	@Override
	public Object getValue() {
		throw new IllegalStateException("The black hole metric does not expect any reading.");
	}
}
