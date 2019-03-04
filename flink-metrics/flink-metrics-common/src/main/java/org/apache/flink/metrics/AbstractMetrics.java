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

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An abstract metrics class that helps creating metrics for components.
 */
public abstract class AbstractMetrics {
	private static final Gauge<?> GAUGE_PLACEHOLDER = (Gauge<Integer>) () -> -1;
	private final MetricGroup metricGroup;
	private final ConcurrentMap<String, Metric> metrics = new ConcurrentHashMap<>();
	private final MetricDef metricDef;
	private final Map<String, Boolean> metricSwitch;

	// --------------- constructors ------------------------

	/**
	 * Construct the metrics based on the given {@link MetricDef}.
	 *
	 * @param metricGroup the metric group to register the metrics.
	 * @param metricDef the metric definition.
	 */
	public AbstractMetrics(MetricGroup metricGroup, MetricDef metricDef) {
		this(metricGroup, metricDef, Collections.emptyMap());
	}

	/**
	 * Construct the metrics based on the given {@link MetricDef}.
	 *
	 * @param metricGroup the metric group to register the metrics.
	 * @param metricDef the metric definition.
	 */
	public AbstractMetrics(MetricGroup metricGroup, MetricDef metricDef, Map<String, Boolean> metricSwitch) {
		this.metricGroup = metricGroup;
		this.metricDef = metricDef;
		this.metricSwitch = new HashMap<>(metricSwitch == null ? Collections.emptyMap() : metricSwitch);
		metricDef.definitions().values().forEach(
			definition -> createMetric(metricGroup, metricDef, definition.name, definition.spec));
	}

	// --------------------- public methods --------------------

	/**
	 * Get the gauge with given name.
	 *
	 * @param metricName the metric name of the gauge.
	 * @return the gauge if exists.
	 * @throws IllegalArgumentException if the metric has not been set.
	 */
	public Gauge<?> getGauge(String metricName) {
		return get(metricName);
	}

	/**
	 * Get the counter with given name.
	 *
	 * @param metricName the metric name of the counter.
	 * @return the counter if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public Counter getCounter(String metricName) {
		return get(metricName);
	}

	/**
	 * Get the meter with given name.
	 *
	 * @param metricName the metric name of the meter.
	 * @return the meter if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public Meter getMeter(String metricName) {
		return get(metricName);
	}

	/**
	 * Get the histogram with given name.
	 *
	 * @param metricName the metric name of the histogram.
	 * @return the histogram if exists.
	 * @throws IllegalArgumentException if the metric does not exist.
	 */
	public org.apache.flink.metrics.Histogram getHistogram(String metricName) {
		return get(metricName);
	}

	/**
	 * Set the actual gauge.
	 *
	 * @param metricName the name of the gauge.
	 * @param gauge the actual gauge instance.
	 */
	public void setGauge(String metricName, Gauge<?> gauge) {
		// Set the gauge to black hole if the metric is disabled.
		addMetric(
				metricName,
				isMetricEnabled(metricName) ? metricGroup.gauge(metricName, gauge) : BlackHoleMetric.instance());
	}

	/**
	 * Get a metric. It is a convenient method with adaptive types.
	 *
	 * @param metricName the name of the metric.
	 * @return the metric with the given name.
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(String metricName) {
		T metric = (T) metrics.get(metricName);
		if (metric == GAUGE_PLACEHOLDER) {
			throw new IllegalStateException("Gauge" + metricName + " has not been set.");
		}
		return notNull(metric, "Metric " + metricName + " does not exist.");
	}

	/**
	 * Get the name of all metrics.
	 *
	 * @return a set of all metric names.
	 */
	public Set<String> allMetricNames() {
		return Collections.unmodifiableSet(metrics.keySet());
	}

	/**
	 * Get the metric group in which this abstract metrics is defined.
	 *
	 * @return the metric group associated with this abstract metrics.
	 */
	public MetricGroup metricGroup() {
		return metricGroup;
	}

	// ------------------ private helper methods ---------------------

	private void createMetric(MetricGroup metricGroup, MetricDef metricDef, String metricName, MetricSpec spec) {
		if (metrics.get(metricName) != null) {
			return;
		}

		// Create dependency metrics recursively.
		for (String dependencyMetricName : spec.dependencies) {
			if (!metrics.containsKey(dependencyMetricName)) {
				MetricDef.MetricInfo dependencyMetricInfo = metricDef.definition(dependencyMetricName);
				if (!isMetricEnabled(
						dependencyMetricName,
						"Could not find metric definition for dependency metric " + dependencyMetricName
						+ " of metric " + metricName + ".")) {
					throw new IllegalStateException("Metric " + metricName + " could not be created because "
														+ "dependency metric " + dependencyMetricName + " is disabled.");
				}
				createMetric(metricGroup, metricDef, dependencyMetricName, dependencyMetricInfo.spec);
			}
		}

		// Create metric.
		switch (spec.type) {
			case COUNTER:
				if (!isMetricEnabled(metricName)) {
					metrics.put(metricName, BlackHoleMetric.instance());
				} else if (spec instanceof MetricSpec.InstanceSpec) {
					addMetric(
						metricName,
						metricGroup.counter(metricName,	(Counter) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					addMetric(metricName, metricGroup.counter(metricName));
				}
				break;

			case METER:
				if (!isMetricEnabled(metricName)) {
					metrics.put(metricName, BlackHoleMetric.instance());
				} else if (spec instanceof MetricSpec.InstanceSpec) {
					addMetric(
						metricName,
						metricGroup.meter(metricName, (Meter) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					MetricSpec.MeterSpec meterSpec = (MetricSpec.MeterSpec) spec;
					String counterName = meterSpec.counterMetricName;
					Counter counter;
					if (counterName == null || counterName.isEmpty()) {
						// No separate counter metric is required.
						counter = new SimpleCounter();
					} else {
						// Separate counter metric has been created.
						counter = (Counter) metrics.get(counterName);
					}
					addMetric(
						metricName,
						metricGroup.meter(metricName, new MeterView(counter, meterSpec.timeSpanInSeconds)));
				}
				break;

			case HISTOGRAM:
				if (!isMetricEnabled(metricName)) {
					metrics.put(metricName, BlackHoleMetric.instance());
				} else if (spec instanceof MetricSpec.InstanceSpec) {
					addMetric(
						metricName,
						metricGroup.histogram(metricName, (org.apache.flink.metrics.Histogram) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					addMetric(metricName, metricGroup.histogram(
						metricName,
						new DropwizardHistogramWrapper(new Histogram(new ExponentiallyDecayingReservoir()))));
				}
				break;

			case GAUGE:
				if (!isMetricEnabled(metricName)) {
					// If the gauge instance is specified, replace it with a black hole instance.
					// Otherwise use Gauge placeholder to ensure user has set an instance in the code.
					metrics.put(
							metricName,
							spec instanceof MetricSpec.InstanceSpec ? BlackHoleMetric.instance() : GAUGE_PLACEHOLDER);
				} else if (spec instanceof MetricSpec.InstanceSpec) {
					addMetric(
						metricName,
						(Gauge) metricGroup.gauge(metricName, (Gauge) ((MetricSpec.InstanceSpec) spec).metric));
				} else {
					metrics.put(metricName, GAUGE_PLACEHOLDER);
				}
				break;

			default:
				throw new IllegalArgumentException("Unknown metric type " + spec.type);

		}
	}

	private Metric addMetric(String name, Metric metric) {
		return metrics.compute(name, (n, m) -> {
			if (m != null && m != GAUGE_PLACEHOLDER) {
				throw new IllegalStateException("Metric of name " + n + " already exists. The metric is " + m);
			}
			return metric;
		});
	}

	private boolean isMetricEnabled(String metricName) {
		return isMetricEnabled(metricName, "Metric " + metricName + " is not defined.");
	}

	private boolean isMetricEnabled(String metricName, String notDefinedErrorMsg) {
		MetricDef.MetricInfo metricInfo = metricDef.definition(metricName);
		if (metricInfo == null) {
			throw new IllegalStateException(notDefinedErrorMsg);
		}
		return metricSwitch.getOrDefault(metricName, metricInfo.enabledByDefault);
	}

	private static <T> T notNull(T obj, String errorMsg) {
		if (obj == null) {
			throw new IllegalStateException(errorMsg);
		} else {
			return obj;
		}
	}
}
