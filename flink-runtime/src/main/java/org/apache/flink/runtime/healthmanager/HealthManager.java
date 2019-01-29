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

package org.apache.flink.runtime.healthmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.healthmanager.metrics.HealthManagerMetricGroup;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.metrics.RestServerMetricProvider;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Health manager monitors status of the jobs.
 */
public class HealthManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(HealthManager.class);

	private static final ConfigOption<Long> JOB_CHECK_INTERNAL =
			ConfigOptions.key("healthmanager.job.check.interval.ms").defaultValue(10000L);

	/** Cluster configurations. */
	private Configuration config;

	/** Executor services. */
	private ScheduledExecutorService executorService;

	/** Metric Provider. */
	private MetricProvider metricProvider;

	/** RestServer client. */
	private RestServerClient restServerClient;

	/** All job Running. */
	private Map<JobID, HealthMonitor> jobMonitors = new HashMap<>();

	/** Handler of the job checker timed task. */
	private ScheduledFuture timedTaskHandler;

	/** MetricsGroup for HealthManager. */
	private HealthManagerMetricGroup metricGroup;

	public HealthManager(
			String restServerAddress,
			MetricRegistry metricRegistry,
			Configuration config) throws Exception {

		this.config = config;

		this.executorService = new ScheduledThreadPoolExecutor(
				4, new ExecutorThreadFactory("health-manager"));

		this.metricGroup = new HealthManagerMetricGroup(metricRegistry);

		LOGGER.info("Starting Health manager with rest server:" + restServerAddress);
		this.restServerClient = new RestServerClientImpl(restServerAddress, config, executorService);

		this.metricProvider = new RestServerMetricProvider(config, restServerClient, executorService);
	}

	public void start() {
		LOGGER.info("Starting health manager.");
		long interval = config.getLong(JOB_CHECK_INTERNAL);
		if (interval > 0) {
			timedTaskHandler = executorService.scheduleAtFixedRate(
					new RunningJobListChecker(), 0, interval, TimeUnit.MILLISECONDS);
		}

		this.metricProvider.open();
	}

	public void stop() {
		LOGGER.info("Stopping health manager.");
		if (timedTaskHandler != null) {
			timedTaskHandler.cancel(true);
		}

		metricProvider.close();

		for (HealthMonitor monitor : jobMonitors.values()) {
			monitor.stop();
		}

		jobMonitors.clear();
		executorService.shutdown();

		if (metricGroup != null) {
			metricGroup.close();
		}
	}

	/**
	 * Job checker which starts new HealthMonitor for new jobs and stops HealthMonitor for stopped jobs.
	 */
	private class RunningJobListChecker implements Runnable {

		@Override
		public void run() {

			Map<JobID, String> runningIds = new HashMap<>();

			try {
				restServerClient.listJob()
						.stream()
						.filter(status -> !status.getJobState().isGloballyTerminalState())
						.forEach(status -> runningIds.put(status.getJobId(), status.getJobName()));
			} catch (Throwable e) {
				// skip current round check since some wrong in rest server.
				LOGGER.warn("Wait rest server to be ready", e);
				return;
			}
			try {
				for (JobID id : runningIds.keySet()) {
					if (!jobMonitors.containsKey(id)) {
						LOGGER.info("New job submitted, id:" + id);
						HealthMonitor newMonitor = new HealthMonitor(
								id,
								metricProvider,
								restServerClient,
								metricGroup.addJob(id, runningIds.get(id)),
								executorService, config);
						try {
							newMonitor.start();
						} catch (Exception e) {
							LOGGER.info("Fail to start monitor for job:" + id, e);
							continue;
						}

						jobMonitors.put(id, newMonitor);
					}
				}

				List<JobID> finishedJob = new LinkedList<>();
				for (JobID id : jobMonitors.keySet()) {
					if (!runningIds.containsKey(id)) {
						LOGGER.info("New job finished or failed, id:" + id);
						finishedJob.add(id);
						metricGroup.removeJob(id);
					}
				}

				for (JobID id : finishedJob) {
					jobMonitors.remove(id).stop();
				}
			} catch (Throwable e) {
				LOGGER.warn("Exception caught in job checker", e);
			}
		}
	}
}
