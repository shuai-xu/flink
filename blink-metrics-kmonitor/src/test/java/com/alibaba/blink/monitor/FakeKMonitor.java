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

import com.taobao.kmonitor.KMonitor;
import com.taobao.kmonitor.MetricType;
import com.taobao.kmonitor.PriorityType;
import com.taobao.kmonitor.core.MetricsCollector;
import com.taobao.kmonitor.core.MetricsTags;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * FakeKMonitor for test.
 */
public class FakeKMonitor implements KMonitor {
	private static final Logger LOG = LoggerFactory.getLogger(FakeKMonitor.class);

	private String kMonitorName;

	private static Set<String> registeredMetricNames = new HashSet<>();

	private static Set<String> reportedMetricNames = new HashSet<>();
	private boolean isAllRegistered = false;
	private static boolean isAllReported = false;
	public static final String METRICS_ERROR = "METRICS_ERROR";
	private static final String VERTEX_ID_TAG_NAME = "container_type";
	private static final String TASK_TOPOLOGY_ID_TAG_NAME = "task_topology_id";

	public static String[] suffix = new String[]{"registered", "reported"};
	private static File regFile;
	private static File reportFile;

	static {
		try {
			regFile = File.createTempFile("metrics", "." + suffix[0], new File(System.getProperty("user.dir")));
			reportFile = File.createTempFile("metrics", "." + suffix[1], new File(System.getProperty("user.dir")));
		} catch (IOException e) {
			LOG.error("create File error, {}", e);
		}
	}

	public FakeKMonitor(String kMonitorName) {
		this.kMonitorName = kMonitorName;

	}

	public String name() {
		return this.kMonitorName;
	}

	public void getMetrics(List<PriorityType> priorities, MetricsCollector collector, boolean all) {
		LOG.info("getMetrics {}, {}, {}", priorities, collector, all);
	}

	@Override
	public boolean register(String metricName, MetricType metricType) {
		return this.register(metricName, metricType, 0);
	}

	@Override
	public boolean register(String metricName, MetricType metricType, PriorityType priorityType) {
		return this.register(metricName, metricType, 0, priorityType);
	}

	@Override
	public boolean register(String metricName, MetricType metricType, int statisticsType) {
		return this.register(metricName, metricType, statisticsType, PriorityType.NORMAL);
	}

	@Override
	public boolean register(String metricName, MetricType metricType, int statisticsType, PriorityType priorityType) {
		LOG.info("register {}, {}, {}, {}", metricName, metricType, statisticsType, priorityType);
		saveToFile(regFile, metricName, metricType, new Integer(statisticsType), priorityType);
		registeredMetricNames.add(metricName);
		return false;
	}

	private void saveToFile(File file, Object... objs) {
		StringBuilder sb = new StringBuilder();
		for (Object obj : objs) {
			if (obj != null) {
				sb.append(obj.toString() + ", ");
			}
		}
		saveToFile(file, sb.toString());
	}

	private void saveToFile(File file, String line) {
		try {
			FileUtils.writeStringToFile(file, line + "\n", true);
		} catch (IOException e) {
			LOG.error("Write to File error {}", e);
		}
	}

	private void saveToFile(File file, Set<String> metricsNames) {
		saveToFile(file, metricsNames.toString());
	}

	@Override
	public void report(String metricName, double value) {
		this.report(metricName, (MetricsTags) null, value);
	}

	@Override
	public void report(String metricName, MetricsTags metricsTags, double value) {
		LOG.info("report {}, {}, {}", metricName, metricsTags, value);

		if (!isAllRegistered) {
			isAllRegistered = true;
			saveToFile(regFile, registeredMetricNames);
		}

		saveToFile(reportFile, metricName, metricsTags, new Double(value));
		if (registeredMetricNames.contains(metricName)) {
			reportedMetricNames.add(metricName);
			if (metricName.contains("operator")) {
				if (!verifyTaskMetricsTags(metricsTags)) {
					LOG.error(METRICS_ERROR + ", when report of {}, {}, {}", metricName, metricsTags, value);
					throwMetricsException(metricName + metricsTags.toString() + value);
				}
			}
		}
	}

	private boolean verifyTaskMetricsTags(MetricsTags metricsTags) {
		return metricsTags.getValue(VERTEX_ID_TAG_NAME) != null &&
			metricsTags.getValue(TASK_TOPOLOGY_ID_TAG_NAME) != null &&
			(Double.parseDouble(metricsTags.getValue(VERTEX_ID_TAG_NAME)) ==
				Double.parseDouble(metricsTags.getValue(TASK_TOPOLOGY_ID_TAG_NAME)) ||
				(Double.parseDouble(metricsTags.getValue(VERTEX_ID_TAG_NAME)) == -2 &&
					Double.parseDouble(metricsTags.getValue(TASK_TOPOLOGY_ID_TAG_NAME)) == 0));
	}

	@Override
	public void recycle(String metricName, MetricsTags metricsTags) {
		LOG.info("recycle {}, {}", metricName, metricsTags);
	}

	@Override
	public void unregister(String metricName) {
		LOG.info("unregister {}", metricName);
		if (!isAllReported) {
			isAllReported = true;
			saveToFile(reportFile, reportedMetricNames);
		}
	}

	public void throwMetricsException(String message) {
		throw new RuntimeException(METRICS_ERROR + message);
	}
}
