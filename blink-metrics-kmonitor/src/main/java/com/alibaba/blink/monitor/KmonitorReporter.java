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

package com.alibaba.blink.monitor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.taobao.kmonitor.ImmutableMetricTags;
import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.KMonitorFactory;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.core.MetricsTags;
import com.taobao.kmonitor.impl.KMonitorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * {@link MetricReporter} that exports via KMonitor.
 *
 * <p>In order to enable this reporter, please place the following configuration in flink-conf.yaml:
 * <ul>
 * <li>metrics.reporters: kmonitor</li>
 * <li>metrics.reporter.kmonitor.class: com.alibaba.blink.monitor.KmonitorReporter</li>
 * <li>metrics.reporter.kmonitor.interval: 10 SECONDS</li>
 * </ul>
 */
public class KmonitorReporter implements Scheduled, MetricReporter, CharacterFilter {
	private static final Logger LOG = LoggerFactory.getLogger(KmonitorReporter.class);

	/**
	 * The group name used to register from KMonitor.
	 */
	protected static final String KMONITOR_GROUP_NAME = "blink";

	/**
	 * Names for global tag.
	 */
	protected static final String TAG_KMON_SERVICE_NAME = "kmon_service_name";
	protected static final String TAG_PROJECT_NAME = "project";
	protected static final String TAG_METRICS_CLUSTER_NAME = "cluster";
	protected static final String TAG_CREATOR = "creator";
	protected static final String TAG_QUEUE = "queue";
	protected static final String TAG_CLUSTER_NAME = "session_name";

	/**
	 * Keys used to take out the value from config.
	 */
	protected static final String CONF_PROJECT_NAME = "_PROJECT_NAME";
	protected static final String CONF_METRICS_CLUSTER = "_METRICS_CLUSTER";
	protected static final String CONF_CREATOR = "_CREATOR";
	protected static final String CONF_YARN_QUEUE = "_YARN_QUEUE"; // see {@link AbstractYarnClusterDescriptor#deployInternal}
	protected static final String CONF_CLUSTER_NAME = "_FLINK_CLUSTER_NAME"; // see {@link AbstractYarnClusterDescriptor#deployInternal}

	protected static final String CONF_TAG_VALUE_LENGTH = "tag.value.length";
	protected static final String CONF_LATENCY_AGG_TYPE = "latency.agg";
	protected static final String CONF_ENABLE_HISTOGRAM_STATISTIC = "histogram.statistic.enable";

	protected static final String CONF_TENANT_NAME = "tenant.name";

	/**
	 * Some default values for config item.
	 */
	protected static final int DEFAULT_TAG_VALUE_LENGTH = 64;
	protected static final String DEFAULT_LATENCY_AGG_TYPE = "p99";
	protected static final boolean DEFAULT_ENABLE_HISTOGRAM_STATISTIC = false;

	/**
	 * The map used to map the config key to the global-tag key.
	 */
	private static final Map<String, TagKeyOption> CONF_TO_TAG_KEY_MAP = new HashMap<>();

	static {
		CONF_TO_TAG_KEY_MAP.put(CONF_PROJECT_NAME, new TagKeyOption(TAG_PROJECT_NAME, null));
		CONF_TO_TAG_KEY_MAP.put(CONF_METRICS_CLUSTER, new TagKeyOption(TAG_METRICS_CLUSTER_NAME, null));
		CONF_TO_TAG_KEY_MAP.put(CONF_CREATOR, new TagKeyOption(TAG_CREATOR, null));
		CONF_TO_TAG_KEY_MAP.put(CONF_YARN_QUEUE, new TagKeyOption(TAG_QUEUE, "default"));
		CONF_TO_TAG_KEY_MAP.put(CONF_CLUSTER_NAME, new TagKeyOption(TAG_CLUSTER_NAME, null));
	}

	/**
	 * The metric name of latency in Flink.
	 */
	protected static final String FLINK_METRIC_LATENCY = "latency";

	/**
	 * The delimiter used to join fields.
	 */
	protected static final String FIELD_DELIMITER = ".";

	/**
	 * Some suffixes for Kmonitor metric.
	 */
	protected static final String METER_RATE_SUFFIX = "rate";
	protected static final String METER_COUNT_SUFFIX = "count";
	protected static final String HISTOGRAM_COUNT_SUFFIX = "count";
	protected static final String HISTOGRAM_MAX_SUFFIX = "max";
	protected static final String HISTOGRAM_MIN_SUFFIX = "min";
	protected static final String HISTOGRAM_MEAN_SUFFIX = "mean";
	protected static final String HISTOGRAM_50PERCENTILE_SUFFIX = "50percentile";
	protected static final String HISTOGRAM_75PERCENTILE_SUFFIX = "75percentile";
	protected static final String HISTOGRAM_95PERCENTILE_SUFFIX = "95percentile";
	protected static final String HISTOGRAM_98PERCENTILE_SUFFIX = "98percentile";
	protected static final String HISTOGRAM_99PERCENTILE_SUFFIX = "99percentile";
	protected static final String HISTOGRAM_999PERCENTILE_SUFFIX = "999percentile";

	private static final Set<String> METER_SUFFIXES = new HashSet<>();

	static {
		METER_SUFFIXES.add(METER_RATE_SUFFIX);
		METER_SUFFIXES.add(METER_COUNT_SUFFIX);
	}

	private static final Set<String> HISTOGRAM_SUFFIXES = new HashSet<>();

	static {
		HISTOGRAM_SUFFIXES.add(HISTOGRAM_COUNT_SUFFIX);
		HISTOGRAM_SUFFIXES.add(HISTOGRAM_MAX_SUFFIX);
		HISTOGRAM_SUFFIXES.add(HISTOGRAM_MIN_SUFFIX);
		HISTOGRAM_SUFFIXES.add(HISTOGRAM_MEAN_SUFFIX);
	}

	/**
	 * The Kmonitor client instance.
	 */
	protected KMonitor kMonitor;

	/**
	 * Flag indicating the reporter has been closed or not.
	 */
	private volatile boolean isClosed;

	/**
	 * The map to store the pair key/value of all global tags.
	 */
	private final Map<String, String> globalTagsMap = new HashMap<String, String>();

	/**
	 * Used to make the KMonitor service name.
	 */
	private String metricsClusterName;

	/**
	 * Used to make the metric name and the service name for KMonitor.
	 */
	private String projectName;

	private String flinkClusterName;

	/**
	 * Job id and container type tags for task manager and task metrics.
	 */
	private String jobID;
	private String containerType;

	/**
	 * Variables which's value is from config.
	 */
	private String latencyAggType;
	private int maxTagValueLength;
	private boolean enableHistogramStatistic;

	/**
	 * Internal storage for all metrics in {@link org.apache.flink.runtime.metrics.MetricRegistry}.
	 */
	private final Map<Gauge<?>, Tuple2<String, MetricGroup>> gauges = new HashMap<>();
	private final Map<Counter, Tuple2<String, MetricGroup>> counters = new HashMap<>();
	private final Map<Histogram, Tuple2<String, MetricGroup>> histograms = new HashMap<>();
	private final Map<Meter, Tuple2<String, MetricGroup>> meters = new HashMap<>();

	/**
	 * Utility class to take out value of global tags from config.
	 */
	static class TagKeyOption {
		final String key;
		final String defaultValue;

		TagKeyOption(String key, String defaultValue) {
			this.key = key;
			this.defaultValue = defaultValue;
		}
	}

	public void initKMonitor() {
		this.kMonitor = KMonitorFactory.getKMonitor(KMONITOR_GROUP_NAME);
	}

	public void startKMonitor() {
		KMonitorFactory.start();
	}

	public void stopKMonitor() {
		KMonitorFactory.stop();
	}

	@Override
	public void open(MetricConfig config) {
		this.isClosed = false;
		initKMonitor();

		// set tenant name if config, otherwise is default "blink"
		if (config.containsKey(CONF_TENANT_NAME)) {
			KMonitorConfig.setKMonitorTenantName((String) config.get(CONF_TENANT_NAME));
			LOG.info("Detect user-defined tenant name, switch tenant to {}.", KMonitorConfig.getKMonitorTenantName());
		} else {
			LOG.info("No user-defined tenant name, default tenant is {}.", KMonitorConfig.getKMonitorTenantName());
		}

		// take out values from config
		this.maxTagValueLength = config.getInteger(CONF_TAG_VALUE_LENGTH, DEFAULT_TAG_VALUE_LENGTH);
		this.latencyAggType = config.getString(CONF_LATENCY_AGG_TYPE, DEFAULT_LATENCY_AGG_TYPE);
		this.enableHistogramStatistic = config.getBoolean(CONF_ENABLE_HISTOGRAM_STATISTIC, DEFAULT_ENABLE_HISTOGRAM_STATISTIC);

		if (enableHistogramStatistic) {
			LOG.info("Enable histogram statistic");

			HISTOGRAM_SUFFIXES.add(HISTOGRAM_50PERCENTILE_SUFFIX);
			HISTOGRAM_SUFFIXES.add(HISTOGRAM_75PERCENTILE_SUFFIX);
			HISTOGRAM_SUFFIXES.add(HISTOGRAM_95PERCENTILE_SUFFIX);
			HISTOGRAM_SUFFIXES.add(HISTOGRAM_98PERCENTILE_SUFFIX);
			HISTOGRAM_SUFFIXES.add(HISTOGRAM_99PERCENTILE_SUFFIX);
			HISTOGRAM_SUFFIXES.add(HISTOGRAM_999PERCENTILE_SUFFIX);
		}

		// initialize the map of global tags from the config
		for (Map.Entry<String, TagKeyOption> entry : CONF_TO_TAG_KEY_MAP.entrySet()) {
			TagKeyOption tagKeyOption = entry.getValue();
			globalTagsMap.put(tagKeyOption.key, config.getString(entry.getKey(), tagKeyOption.defaultValue));
		}

		// get the name of the Flink cluster
		this.flinkClusterName = globalTagsMap.get(TAG_CLUSTER_NAME);

		// set variables for making the kmonitor service name
		metricsClusterName = globalTagsMap.get(TAG_METRICS_CLUSTER_NAME);
		projectName = globalTagsMap.get(TAG_PROJECT_NAME);

		// get the names of job id and container type
		this.jobID = System.getenv("_FLINK_JOB_ID");
		this.containerType = System.getenv("_FLINK_CONTAINER_TYPE");

		// start the kmonitor client
		startKMonitor();

		LOG.info("Start the kmonitor client with {groupName: {}, metricsClusterName: {}, projectName: {}}",
			KMONITOR_GROUP_NAME, metricsClusterName, projectName);
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			Tuple2<String, MetricGroup> metricNameGroup = new Tuple2<>(metricName, group);
			if (metric instanceof Counter) {
				counters.put((Counter) metric, metricNameGroup);
				kMonitor.register(formMetricName(metricNameGroup), MetricType.RAW);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Register metric with name : {}", formMetricName(metricNameGroup));
				}
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, metricNameGroup);
				kMonitor.register(formMetricName(metricNameGroup), MetricType.RAW);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Register metric with name : {}", formMetricName(metricNameGroup));
				}
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, metricNameGroup);
				for (String histogramSuffix : HISTOGRAM_SUFFIXES) {
					kMonitor.register(prefix(formMetricName(metricNameGroup), histogramSuffix), MetricType.RAW);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Register metric with name : {}", prefix(formMetricName(metricNameGroup), histogramSuffix));
					}
				}
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, metricNameGroup);

				kMonitor.register(formMetricName(metricNameGroup), MetricType.RAW);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Register QPS metric with name : {}", formMetricName(metricNameGroup));
				}

				for (String meterSuffix : METER_SUFFIXES) {
					kMonitor.register(prefix(formMetricName(metricNameGroup), meterSuffix), MetricType.RAW);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Register metric with name : {}", prefix(formMetricName(metricNameGroup), meterSuffix));
					}
				}
			} else {
				LOG.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			Tuple2<String, MetricGroup> metricNameGroup = new Tuple2<>(metricName, group);
			if (metric instanceof Counter) {
				counters.remove(metric);
				kMonitor.unregister(formMetricName(metricNameGroup));
			} else if (metric instanceof Gauge) {
				gauges.remove(metric);
				kMonitor.unregister(formMetricName(metricNameGroup));
			} else if (metric instanceof Histogram) {
				histograms.remove(metric);
				for (String histogramSuffix : HISTOGRAM_SUFFIXES) {
					kMonitor.unregister(prefix(formMetricName(metricNameGroup), histogramSuffix));
				}
			} else if (metric instanceof Meter) {
				meters.remove(metric);
				for (String meterSuffix : METER_SUFFIXES) {
					kMonitor.unregister(prefix(formMetricName(metricNameGroup), meterSuffix));
				}
			} else {
				LOG.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void report() {
		// instead of locking here, we tolerate exceptions
		// we do this to prevent holding the lock for very long and blocking
		// operator creation and shutdown
		try {
			// report all gauges
			for (Map.Entry<Gauge<?>, Tuple2<String, MetricGroup>> entry : gauges.entrySet()) {
				if (isClosed) {
					return;
				}
				Tuple2<String, MetricGroup> metricInfo = entry.getValue();

				reportGauge(formMetricName(metricInfo),
					formMetricTags(metricInfo.f1, metricInfo.f0), entry.getKey());
			}

			// report all counters
			for (Map.Entry<Counter, Tuple2<String, MetricGroup>> entry : counters.entrySet()) {
				if (isClosed) {
					return;
				}
				reportCounter(formMetricName(entry.getValue()),
					formMetricTags(entry.getValue().f1, entry.getValue().f0), entry.getKey());
			}

			// report all histograms
			for (Map.Entry<Histogram, Tuple2<String, MetricGroup>> entry : histograms.entrySet()) {
				if (isClosed) {
					return;
				}

				/**
				 * Specially handles for the job's Flink latency (see {@link AbstractStreamOperator#latencyStats}).
				 */
				Tuple2<String, MetricGroup> metricInfo = entry.getValue();
				String metricName = formMetricName(metricInfo);

				MetricHierarchy metricHierarchy = getMetricHierarchy(metricInfo.f1);
				if (metricHierarchy.isJob() && formJobFlinkLatencyMetricName(metricHierarchy.getId()).equals(metricName)) {
					reportJobFlinkLatency(metricName,
						formMetricTags(metricInfo.f1, metricInfo.f0), entry.getKey());
				} else {
					reportHistogram(formMetricName(entry.getValue()),
						formMetricTags(entry.getValue().f1, entry.getValue().f0), entry.getKey());
				}
			}

			// report all meters
			for (Map.Entry<Meter, Tuple2<String, MetricGroup>> entry : meters.entrySet()) {
				if (isClosed) {
					return;
				}
				reportMeter(formMetricName(entry.getValue()),
					formMetricTags(entry.getValue().f1, entry.getValue().f0), entry.getKey());
			}
		} catch (ConcurrentModificationException | NoSuchElementException e) {
			// ignore - may happen when metrics are concurrently added or removed
			// report next time
		}
	}

	@Override
	public String filterCharacters(String str) {
		return filterCharacters(str, Integer.MAX_VALUE);
	}

	@Override
	public void close() {
		isClosed = true;

		stopKMonitor();

		LOG.info("Stop the kmonitor client: {groupName: {}, metricsClusterName: {}, projectName: {}}",
			KMONITOR_GROUP_NAME, metricsClusterName, projectName);
	}

	public int getMaxTagValueLength() {
		return maxTagValueLength;
	}

	// -------------------------------------------------------------------------------
	//  Methods which can be overridden in subclass
	// -------------------------------------------------------------------------------

	/**
	 * Always assemble the metric name for KMonitor in the form of $projectName.$jobName.[chain<$groupName>.]$metricName or
	 * $projectName.$clusterId.[chain<$groupName>.]$metricName.
	 *
	 * @param metricInfo Metric name and its MetricGroup instance
	 * @return Metric name prefix
	 */
	protected String formMetricName(Tuple2<String, MetricGroup> metricInfo) {
		StringBuilder strBuf = new StringBuilder();

		// append the project name
		if (projectName != null) {
			strBuf.append(projectName).append(FIELD_DELIMITER);
		}

		// append the job name or the cluster name
		MetricHierarchy metricHierarchy = getMetricHierarchy(metricInfo.f1);

		strBuf.append(filterCharacters(metricHierarchy.getId())).append(FIELD_DELIMITER);

		// append all group names in order by inheritance
		Preconditions.checkState(metricInfo.f1 instanceof FrontMetricGroup,
			"The MetricGroup passed by calling MetricReporter.notifyOfAddedMetric() should be a FrontMetricGroup instance.");

		QueryScopeInfo info = ((FrontMetricGroup) metricInfo.f1).getReference().getQueryServiceMetricInfo(this);
		if (info.scope != null && !info.scope.isEmpty()) {
			strBuf.append(info.scope).append(".");
		}

		// append the metric name
		if (metricInfo.f0 != null && !metricInfo.f0.isEmpty()) {
			strBuf.append(filterCharacters(metricInfo.f0));
		}

		return strBuf.toString();
	}

	protected MetricsTags formMetricTags(MetricGroup group, String metricName) {
		Map<String, String> metricTagsMap = new HashMap<>();

		/** Add the tag {@link #TAG_KMON_SERVICE_NAME}. */
		MetricHierarchy metricHierarchy = getMetricHierarchy(group);

		String kmonServiceName = makeKmonServiceName(metricsClusterName, projectName, metricHierarchy.getId());
		if (kmonServiceName != null) {
			metricTagsMap.put(filterCharacters(TAG_KMON_SERVICE_NAME), filterCharacters(kmonServiceName));
		}

		// add all global tags
		for (Map.Entry<String, String> entry : globalTagsMap.entrySet()) {
			String value = entry.getValue();
			if (value != null) {
				metricTagsMap.put(filterCharacters(entry.getKey()), filterCharacters(value));
			}
		}

		/** Add all scope variables as metric tags. */
		Map<String, String> variables = group.getAllVariables();
		for (Map.Entry<String, String> entry : variables.entrySet()) {
			// Use variable name without scope prefix and suffix as tag name
			metricTagsMap.put(
				filterCharacters(entry.getKey().substring(1, entry.getKey().length() - 1)),
				filterCharacters(entry.getValue(), maxTagValueLength));
		}

		// add job id and container type tags
		if (jobID != null) {
			metricTagsMap.put("job_id", jobID);
		}
		if (containerType != null) {
			metricTagsMap.put("container_type", containerType);
		}

		return new ImmutableMetricTags(metricTagsMap);
	}

	protected String filterCharacters(String str, int maxLength) {
		char[] chars = null;
		final int strLen = str.length();
		int pos = 0;

		for (int i = 0; i < strLen && i < maxLength; i++) {
			final char c = str.charAt(i);
			if (!((c >= '0' && c <= '9')
				|| (c >= 'a' && c <= 'z')
				|| (c >= 'A') && c <= 'Z')) {
				if (chars == null) {
					chars = str.toCharArray();
				}
				chars[pos] = '_';
			} else if (chars != null) {
				if (chars != null) {
					chars[pos] = c;
				}
			}
			pos++;
		}

		return chars == null ? str.substring(0, maxLength > strLen ? strLen : maxLength) : new String(chars, 0, pos);
	}

	// -------------------------------------------------------------------------------
	// Helper Methods
	// -------------------------------------------------------------------------------

	private void reportJobFlinkLatency(String name, MetricsTags tags, Histogram histogram) {
		if (name == null || tags == null || histogram == null) {
			return;
		}

		HistogramStatistics statistics = histogram.getStatistics();
		if (statistics != null) {
			double latency = 0;
			switch (latencyAggType) {
				case "p99":
					latency = statistics.getQuantile(0.99);
					break;
				case "p95":
					latency = statistics.getQuantile(0.95);
					break;
				case "p50":
					latency = statistics.getQuantile(0.5);
					break;
				case "max":
					latency = statistics.getMax();
					break;
				case "mean":
					latency = statistics.getMean();
					break;
				case "min":
					latency = statistics.getMin();
					break;
			}

			send(name, tags, latency);
		}
	}

	private void reportCounter(final String name, final MetricsTags tags, final Counter counter) {
		if (name == null || tags == null || counter == null) {
			return;
		}

		send(name, tags, counter.getCount());
	}

	private void reportGauge(final String name, final MetricsTags tags, final Gauge<?> gauge) {
		if (name == null || tags == null || gauge == null) {
			return;
		}

		try {
			send(name, tags, Double.valueOf(gauge.getValue().toString()));
		} catch (NumberFormatException e) {
			// ignore gauge values which can not be converted into double
		}
	}

	private void reportHistogram(final String name, final MetricsTags tags, final Histogram histogram) {
		if (name == null || tags == null || histogram == null) {
			return;
		}

		HistogramStatistics statistics = histogram.getStatistics();
		if (statistics != null) {
			send(prefix(name, HISTOGRAM_COUNT_SUFFIX), tags, histogram.getCount());
			send(prefix(name, HISTOGRAM_MAX_SUFFIX), tags, statistics.getMax());
			send(prefix(name, HISTOGRAM_MIN_SUFFIX), tags, statistics.getMin());
			send(prefix(name, HISTOGRAM_MEAN_SUFFIX), tags, statistics.getMean());
			if (enableHistogramStatistic) {
				send(prefix(name, HISTOGRAM_50PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.5));
				send(prefix(name, HISTOGRAM_75PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.75));
				send(prefix(name, HISTOGRAM_95PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.95));
				send(prefix(name, HISTOGRAM_98PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.98));
				send(prefix(name, HISTOGRAM_99PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.99));
				send(prefix(name, HISTOGRAM_999PERCENTILE_SUFFIX), tags, statistics.getQuantile(0.999));
			}
		}
	}

	private void reportMeter(final String name, final MetricsTags tags, final Meter meter) {
		if (name == null || tags == null || meter == null) {
			return;
		}

		send(name, tags, meter.getRate());
		send(prefix(name, METER_RATE_SUFFIX), tags, meter.getRate());
		send(prefix(name, METER_COUNT_SUFFIX), tags, meter.getCount());
	}

	private void send(final String name, MetricsTags tags, final double value) {
		kMonitor.report(name, tags, value);
		if (LOG.isDebugEnabled()) {
			LOG.debug(name + ":" + tags.getUnmodifiableTags() + ":" + value);
		}
	}

	private String prefix(String... names) {
		if (names.length > 0) {
			StringBuilder stringBuilder = new StringBuilder(names[0]);

			for (int i = 1; i < names.length; i++) {
				stringBuilder.append(FIELD_DELIMITER).append(names[i]);
			}

			return stringBuilder.toString();
		} else {
			return "";
		}
	}

	private MetricHierarchy getMetricHierarchy(MetricGroup group) {
		String hierarchyId = null;
		boolean isJob = false;

		Map<String, String> allVariables = group.getAllVariables();
		if (allVariables.containsKey(ScopeFormat.SCOPE_JOB_NAME)) {
			isJob = true;
			hierarchyId = allVariables.get(ScopeFormat.SCOPE_JOB_NAME);
		} else {
			hierarchyId = this.flinkClusterName;
		}

		Preconditions.checkNotNull(hierarchyId, "hierarchyId must not be null.");
		return new MetricHierarchy(hierarchyId, isJob);
	}

	private static String makeKmonServiceName(String... parts) {
		StringBuffer strBuf = new StringBuffer();
		for (String part : parts) {
			if (part == null) {
				continue;
			}

			if (strBuf.length() > 0) {
				strBuf.append(FIELD_DELIMITER);
			}
			strBuf.append(part);
		}

		String kmonServiceName = strBuf.toString();

		/** compatible with old version */
		if (StringUtils.isNullOrWhitespaceOnly(kmonServiceName)) {
			kmonServiceName = "default";
		}
		return kmonServiceName;
	}

	/**
	 * The logic here should be consistent with ${@link #formMetricName}.
	 *
	 * @param jobName
	 * @return the formed name for the job's Flink latency
	 */
	private String formJobFlinkLatencyMetricName(String jobName) {
		Preconditions.checkNotNull(jobName, "jobName must not be null.");

		StringBuilder strBuf = new StringBuilder();

		// append the project name
		if (projectName != null) {
			strBuf.append(projectName).append(FIELD_DELIMITER);
		}

		// append the job name
		strBuf.append(jobName).append(FIELD_DELIMITER);

		// append the name of the latency metric
		strBuf.append(FLINK_METRIC_LATENCY);

		return strBuf.toString();
	}

	// -------------------------------------------------------------------------------
	// Utility Classes
	// -------------------------------------------------------------------------------

	static class MetricHierarchy {
		private final String id;
		private final boolean isJob;

		public MetricHierarchy(String id, boolean isJob) {
			this.id = id;
			this.isJob = isJob;
		}

		public String getId() {
			return id;
		}

		public boolean isJob() {
			return isJob;
		}
	}
}
