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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.healthmanager.metrics.HealthMonitorMetricGroup;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.ActionSelector;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.Resolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;
import org.apache.flink.runtime.healthmanager.plugins.actionselectors.FirstValidActionSelector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.BackPressureDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DelayIncreasingDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DirectOOMDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FailoverDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FrequentFullGCDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HeapOOMDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LowDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.MemoryOveruseDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.OverParallelizedDetector;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.DirectMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.HeapMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.NativeMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.ParallelismScaler;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Health Monitor which checks status of job periodically, and solve abnormal symptoms when detected.
 */
public class HealthMonitor {

	private static final Logger LOGGER = LoggerFactory.getLogger(HealthManager.class);

	private static final ConfigOption<Long> HEALTH_CHECK_INTERNAL =
			ConfigOptions.key("healthmonitor.health.check.interval.ms").defaultValue(10000L);

	private static final ConfigOption<String> ACTION_SELECTOR_CLASS =
			ConfigOptions.key("healthmonitor.action.selector.class")
					.defaultValue(FirstValidActionSelector.class.getCanonicalName());

	public static final ConfigOption<String> DETECTOR_CLASSES =
			ConfigOptions.key("healthmonitor.detector.classes")
					.defaultValue(HeapOOMDetector.class.getCanonicalName() + ","
						+ DirectOOMDetector.class.getCanonicalName() + ","
						+ FrequentFullGCDetector.class.getCanonicalName() + ","
						+ MemoryOveruseDetector.class.getCanonicalName() + ","
						+ HighDelayDetector.class.getCanonicalName() + ","
						+ LowDelayDetector.class.getCanonicalName() + ","
						+ DelayIncreasingDetector.class.getCanonicalName() + ","
						+ OverParallelizedDetector.class.getCanonicalName() + ","
						+ FailoverDetector.class.getCanonicalName() + ","
						+ BackPressureDetector.class.getCanonicalName());

	public static final ConfigOption<String> RESOLVER_CLASSES =
			ConfigOptions.key("healthmonitor.resolver.classes")
					.defaultValue(HeapMemoryAdjuster.class.getCanonicalName() + ","
						+ DirectMemoryAdjuster.class.getCanonicalName() + ","
						+ NativeMemoryAdjuster.class.getCanonicalName() + ","
						+ ParallelismScaler.class.getCanonicalName());

	private JobID jobID;
	private Configuration config;

	private RestServerClient.JobConfig jobConfig;

	private MetricProvider metricProvider;
	private RestServerClient restServerClient;
	private HealthMonitorMetricGroup metricGroup;
	private ScheduledExecutorService executorService;

	private ScheduledFuture timedTaskHandler;
	private List<Detector> detectors;
	private List<Resolver> resolvers;
	private ActionSelector actionSelector;

	private volatile long lastExecution = 0;

	private volatile long successActionCount = 0;
	private volatile long failedActionCount = 0;

	@VisibleForTesting
	public HealthMonitor(
			JobID jobID,
			MetricProvider metricProvider,
			RestServerClient visitor,
			ScheduledExecutorService executorService,
			Configuration config) {
		this(jobID, metricProvider, visitor, null, executorService, config);
	}

	public HealthMonitor(
			JobID jobID,
			MetricProvider metricProvider,
			RestServerClient visitor,
			HealthMonitorMetricGroup metricGroup,
			ScheduledExecutorService executorService,
			Configuration config) {

		this.jobID = jobID;
		this.executorService = executorService;
		this.metricProvider = metricProvider;
		this.restServerClient = visitor;
		this.config = config.clone();
		this.metricGroup = metricGroup;
	}

	public void start() throws Exception {

		LOGGER.info("Starting to monitor job {}", jobID);

		for (String key : getJobConfig().getConfig().keySet()) {
			this.config.setString(key , getJobConfig().getConfig().getString(key, null));
		}

		long checkInterval = config.getLong(HEALTH_CHECK_INTERNAL);

		if (checkInterval > 0) {
			loadDetectors();
			loadResolvers();
			loadActionSelector();
			timedTaskHandler = executorService.scheduleAtFixedRate(
					new HealthChecker(), 0, checkInterval, TimeUnit.MILLISECONDS);
		}

		if (metricGroup != null) {
			MetricGroup actionMetrics = metricGroup.addGroup("action");
			actionMetrics.gauge("success", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return successActionCount;
				}
			});
			actionMetrics.gauge("failure", new Gauge<Long>() {
				@Override
				public Long getValue() {
					return failedActionCount;
				}
			});
		}
	}

	public void stop() {

		if (timedTaskHandler != null) {
			timedTaskHandler.cancel(true);
		}

		if (this.actionSelector != null) {
			this.actionSelector.close();
		}

		for (Detector detector : detectors) {
			detector.close();
		}
		detectors.clear();

		for (Resolver resolver : resolvers) {
			resolver.close();
		}
		resolvers.clear();

	}

	private void loadActionSelector() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		this.actionSelector =
				(ActionSelector) Class.forName(config.getString(ACTION_SELECTOR_CLASS)).newInstance();
		LOGGER.info("Load action selector:" + actionSelector);
		this.actionSelector.open(this);
	}

	private void loadDetectors() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		String[] detectorClazzs = config.getString(DETECTOR_CLASSES).split(",");
		LOGGER.info("Load detectors:" + StringUtils.join(detectorClazzs, ","));
		this.detectors = new ArrayList<>(detectorClazzs.length);
		for (String clazz : detectorClazzs) {
			Detector detector = (Detector) Class.forName(clazz.trim()).newInstance();
			detectors.add(detector);

			detector.open(this);
		}

	}

	private void loadResolvers() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		String[] clazzs = config.getString(RESOLVER_CLASSES).split(",");
		LOGGER.info("Load resolvers:" + StringUtils.join(clazzs, ","));
		this.resolvers = new ArrayList<>(clazzs.length);
		for (String clazz : clazzs) {
			Resolver resolver = (Resolver) Class.forName(clazz.trim()).newInstance();
			resolvers.add(resolver);
			resolver.open(this);
		}
	}

	public JobID getJobID() {
		return jobID;
	}

	public MetricProvider getMetricProvider() {
		return metricProvider;
	}

	public RestServerClient getRestServerClient() {
		return restServerClient;
	}

	public Configuration getConfig() {
		return config;
	}

	public ScheduledExecutorService getExecutorService() {
		return executorService;
	}

	public RestServerClient.JobConfig getJobConfig() {
		if (jobConfig == null) {
			jobConfig = restServerClient.getJobConfig(jobID);
		}
		return jobConfig;
	}

	public long getLastExecution() {
		return lastExecution;
	}

	/**
	 * Health check for a job, which detects abnormal symptoms of job which detectors and tries to
	 * resolve abnormal status with registered Resolver.
	 */
	public class HealthChecker implements Runnable {

		@Override
		public void run() {

			LOGGER.debug("Start to check job {}.", jobID);

			List<Symptom> symptoms = new LinkedList<>();

			// 1. check abnormal symptoms.
			for (Detector detector: detectors) {
				Symptom symptom = null;
				try {
					symptom = detector.detect();
				} catch (Throwable e) {
					LOGGER.warn("Exception caught in detector " + detector, e);
				}
				if (symptom != null) {
					symptoms.add(symptom);
				}
			}
			LOGGER.debug("Detected symptoms: {}.", symptoms);

			// 2. diagnose and generate resolve action.
			List<Action> actions = new LinkedList<>();
			for (Resolver resolver : resolvers) {
				try {
					Action action = resolver.resolve(symptoms);
					if (action != null) {
						actions.add(action);
					}
				} catch (Throwable e) {
					LOGGER.warn("Exception caught in resolver " + resolver, e);
				}
			}
			LOGGER.debug("Generated actions: {}.", actions);

			if (actions.size() == 0) {
				return;
			}

			// 3. select an action to execute.
			Action action = null;
			try {
				action = actionSelector.accept(actions);
			} catch (Throwable e) {
				LOGGER.warn("Exception caught in action selector", e);
			}
			LOGGER.info("Selected action: {}.", action);

			if (action != null) {
				try {

					// reset job config.
					jobConfig = null;

					// 4. execute an action.
					LOGGER.info("Executing action {}", action);

					action.execute(restServerClient);

					// 5. validate result of action.
					if (!action.validate(metricProvider, restServerClient)) {

						LOGGER.info("Action {} validate failed, try to roll back", action);

						// 6. execution roll back action if validation failed.
						action.rollback().execute(restServerClient);

						// 7. notify failure of an action.
						actionSelector.actionFailed(action);
						failedActionCount++;
					} else {
						LOGGER.info("Action {} validate succeed.", action);

						// 6. notify success of an action.
						actionSelector.actionSucceed(action);
						successActionCount++;
					}
				} catch (Throwable e) {
					LOGGER.warn("Action " + action + " execution failed.", e);
					actionSelector.actionFailed(action);
					failedActionCount++;
				}

				lastExecution = System.currentTimeMillis();
			}

		}
	}
}
