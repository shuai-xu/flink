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

package org.apache.flink.runtime.healthmanager.metrics;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.commons.lang3.NotImplementedException;

import java.util.List;


/**
 * Metrics aggregation function.
 */
public abstract class MetricAggFunction {

	public abstract Tuple2<Long, Double> getValue(List<Tuple2<Long, Double>> values);

	public static MetricAggFunction getMetricAggFunction(MetricAggType aggType) {
		switch (aggType) {
			case SUM:
				return Sum.SUM_FUNCTION;
			case MIN:
				return Min.MIN_FUNCTION;
			case MAX:
				return Max.MAX_FUNCTION;
			case AVG:
				return Avg.AVG_FUNCTION;
			default:
				throw new NotImplementedException("Agg type:" + aggType);
		}
	}

	private static class Sum extends MetricAggFunction {

		private static final Sum SUM_FUNCTION = new Sum();

		@Override
		public Tuple2<Long, Double> getValue(List<Tuple2<Long, Double>> values) {
			long timestamp = -1;
			double value = 0;
			for (Tuple2<Long, Double> timestampAndValue : values) {
				if (timestampAndValue == null) {
					return null;
				} else if (timestamp == -1) {
					timestamp = timestampAndValue.f0;
				} else if (timestamp != timestampAndValue.f0) {
					// not all sub metric aligned.
					return null;
				}
				value += timestampAndValue.f1;
			}
			if (timestamp == -1) {
				return null;
			} else {
				return Tuple2.of(timestamp, value);
			}
		}
	}

	private static class Min extends MetricAggFunction {

		private static final Min MIN_FUNCTION = new Min();

		@Override
		public Tuple2<Long, Double> getValue(List<Tuple2<Long, Double>> values) {
			long timestamp = -1;
			double value = Double.MAX_VALUE;
			for (Tuple2<Long, Double> timestampAndValue : values) {
				if (timestampAndValue == null) {
					return null;
				} else if (timestamp == -1) {
					timestamp = timestampAndValue.f0;
				} else if (timestamp != timestampAndValue.f0) {
					// not all sub metric aligned.
					return null;
				}
				if (value > timestampAndValue.f1) {
					value = timestampAndValue.f1;
				}
			}
			if (timestamp == -1) {
				return null;
			} else {
				return Tuple2.of(timestamp, value);
			}
		}
	}

	private static class Max extends MetricAggFunction {

		private static final Max MAX_FUNCTION = new Max();

		@Override
		public Tuple2<Long, Double> getValue(List<Tuple2<Long, Double>> values) {
			long timestamp = -1;
			double value = Double.NEGATIVE_INFINITY;
			for (Tuple2<Long, Double> timestampAndValue : values) {
				if (timestampAndValue == null) {
					return null;
				} else if (timestamp == -1) {
					timestamp = timestampAndValue.f0;
				} else if (timestamp != timestampAndValue.f0) {
					// not all sub metric aligned.
					return null;
				}
				if (value < timestampAndValue.f1) {
					value = timestampAndValue.f1;
				}
			}
			if (timestamp == -1) {
				return null;
			} else {
				return Tuple2.of(timestamp, value);
			}
		}
	}

	private static class Avg extends Sum {

		private static final Avg AVG_FUNCTION = new Avg();

		@Override
		public Tuple2<Long, Double> getValue(List<Tuple2<Long, Double>> values) {
			Tuple2<Long, Double> sum = super.getValue(values);
			if (sum == null) {
				return sum;
			} else {
				return Tuple2.of(sum.f0, sum.f1 / values.size());
			}
		}
	}
}
