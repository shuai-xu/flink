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
import org.apache.flink.runtime.jobgraph.JobStatus;
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
import java.util.stream.Collectors;

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

	private ScheduledFuture timedTaskHandler;

	public HealthManager(Configuration config, String restServerAddress) throws Exception {

		this.config = config;

		this.executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));

		this.restServerClient = new RestServerClientImpl(restServerAddress, config, executorService);

		this.metricProvider = new RestServerMetricProvider(config, restServerClient, executorService);
	}

	public void start() {
		LOGGER.info("Starting health manager.");
		long interval = config.getLong(JOB_CHECK_INTERNAL);
		timedTaskHandler = executorService.scheduleAtFixedRate(
				new RunningJobListChecker(), 0, interval, TimeUnit.MILLISECONDS);
	}

	public void stop() {
		LOGGER.info("Stopping health manager.");
		if (timedTaskHandler != null) {
			timedTaskHandler.cancel(true);
		}
		for (HealthMonitor monitor : jobMonitors.values()) {
			monitor.stop();
		}
		jobMonitors.clear();
		executorService.shutdown();
	}

	/**
	 * Job checker which starts new HealthMonitor for new jobs and stops HealthMonitor for stopped jobs.
	 */
	private class RunningJobListChecker implements Runnable {

		@Override
		public void run() {

			List<JobID> runningIds = null;

			try {
				runningIds = restServerClient.listJob()
						.stream()
						.filter(status -> status.getJobState().equals(JobStatus.RUNNING))
						.map(status -> status.getJobId()).collect(Collectors.toList());
			} catch (Exception e) {
				// skip current round check since some wrong in rest server.
				LOGGER.warn("Wait rest server to be ready", e);
				return;
			}

			for (JobID id: runningIds) {
				if (!jobMonitors.containsKey(id)) {
					LOGGER.info("New job submitted, id:" + id);
					HealthMonitor newMonitor = new HealthMonitor(
							id, metricProvider, restServerClient, executorService, config);
					try {
						newMonitor.start();
					} catch (Exception e) {
						LOGGER.info("Fail to start monitor for job:" + id);
						continue;
					}

					jobMonitors.put(id, newMonitor);
				}
			}

			List<JobID> finishedJob = new LinkedList<>();
			for (JobID id : jobMonitors.keySet()) {
				if (!runningIds.contains(id)) {
					LOGGER.info("New job finished or failed, id:" + id);
					finishedJob.add(id);
				}
			}

			for (JobID id : finishedJob) {
				jobMonitors.remove(id).stop();
			}
		}
	}
}
