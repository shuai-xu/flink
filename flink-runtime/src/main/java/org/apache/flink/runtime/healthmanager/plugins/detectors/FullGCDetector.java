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

package org.apache.flink.runtime.healthmanager.plugins.detectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.JobTMMetricSubscription;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.timeline.TimelineAggType;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.symptoms.JobVertexFullGC;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FullGCDetector detects full GCs a job.
 */
public class FullGCDetector implements Detector {

	private static final String FULL_GC_COUNT_METRIC = "Status.JVM.GarbageCollector.ConcurrentMarkSweep.Count";

	private static final ConfigOption<Integer> FULL_GC_COUNT_THRESHOLD =
		ConfigOptions.key("healthmonitor.full-gc-detector.threshold").defaultValue(0);
	private static final ConfigOption<Long> FULL_GC_CHECK_INTERVAL =
		ConfigOptions.key("healthmonitor.full-gc-detector.interval.ms").defaultValue(5 * 60 * 1000L);

	private JobID jobID;
	private RestServerClient restServerClient;
	private MetricProvider metricProvider;

	private int gcCountThreshold;
	private long gcCheckInterval;

	private JobTMMetricSubscription gcMetricMaxSubscription;
	private JobTMMetricSubscription gcMetricMinSubscription;

	@Override
	public void open(HealthMonitor monitor) {

		jobID = monitor.getJobID();
		restServerClient = monitor.getRestServerClient();
		metricProvider = monitor.getMetricProvider();

		gcCountThreshold = monitor.getConfig().getInteger(FULL_GC_COUNT_THRESHOLD);
		gcCheckInterval = monitor.getConfig().getLong(FULL_GC_CHECK_INTERVAL);

		gcMetricMaxSubscription = metricProvider.subscribeAllTMMetric(jobID, FULL_GC_COUNT_METRIC, gcCheckInterval, TimelineAggType.MAX);
		gcMetricMinSubscription = metricProvider.subscribeAllTMMetric(jobID, FULL_GC_COUNT_METRIC, gcCheckInterval, TimelineAggType.MIN);
	}

	@Override
	public void close() {
		if (metricProvider != null && gcMetricMinSubscription != null) {
			metricProvider.unsubscribe(gcMetricMinSubscription);
			gcMetricMinSubscription = null;
		}

		if (metricProvider != null && gcMetricMaxSubscription != null) {
			metricProvider.unsubscribe(gcMetricMaxSubscription);
			gcMetricMaxSubscription = null;
		}
	}

	@Override
	public Symptom detect() {
		long now = System.currentTimeMillis();

		Map<String, Tuple2<Long, Double>> maxGC = gcMetricMaxSubscription.getValue();
		Map<String, Tuple2<Long, Double>> minGC = gcMetricMinSubscription.getValue();
		if (maxGC == null || maxGC.isEmpty() || minGC == null || minGC.isEmpty() || !maxGC.keySet().equals(minGC.keySet())) {
			return null;
		}

		Set<JobVertexID> jobVertexIDs = new HashSet<>();
		for (String tmId : maxGC.keySet()) {
			if (now - maxGC.get(tmId).f0 > gcCheckInterval * 2 || now - minGC.get(tmId).f0 > gcCheckInterval * 2) {
				continue;
			}

			double deltaGCCount = maxGC.get(tmId).f1 - minGC.get(tmId).f1;

			if (deltaGCCount > gcCountThreshold) {
				List<JobVertexID> jvIds = restServerClient.getTaskManagerTasks(tmId);
				if (jvIds != null) {
					jobVertexIDs.addAll(jvIds);
				}
			}
		}

		if (jobVertexIDs != null && !jobVertexIDs.isEmpty()) {
			return new JobVertexFullGC(jobID, new ArrayList<>(jobVertexIDs));
		}
		return null;
	}
}
