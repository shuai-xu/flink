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
import org.apache.flink.runtime.healthmanager.plugins.actionselectors.RescaleResourcePriorActionSelector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DelayIncreasingDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.DirectOOMDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FailoverDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.FrequentFullGCDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HeapOOMDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighCpuDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighDelayDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.HighNativeMemoryDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.JobStableDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.JobStuckDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.KilledDueToMemoryExceedDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LongTimeFullGCDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LowCpuDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.LowMemoryDetector;
import org.apache.flink.runtime.healthmanager.plugins.detectors.OverParallelizedDetector;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.CpuAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.DirectMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.HeapMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.NativeMemoryAdjuster;
import org.apache.flink.runtime.healthmanager.plugins.resolvers.ParallelismScaler;
import org.apache.flink.runtime.healthmanager.plugins.utils.HealthMonitorOptions;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Health Monitor which checks status of job periodically, and solve abnormal symptoms when detected.
 */
public class HealthMonitor {

	private static final Logger LOGGER = LoggerFactory.getLogger(HealthMonitor.class);

	public static final ConfigOption<Boolean> HEALTH_MONITOR_ENABLED =
			ConfigOptions.key("healthmonitor.enabled").defaultValue(true);

	public static final ConfigOption<Long> HEALTH_CHECK_INTERNAL =
			ConfigOptions.key("healthmonitor.health.check.interval.ms").defaultValue(10000L);

	public static final ConfigOption<String> ACTION_SELECTOR_CLASS =
			ConfigOptions.key("healthmonitor.action.selector.class")
					.defaultValue(RescaleResourcePriorActionSelector.class.getCanonicalName());

	public static final ConfigOption<String> DETECTOR_CLASSES =
			ConfigOptions.key("healthmonitor.detector.classes").noDefaultValue();

	public static final ConfigOption<String> RESOLVER_CLASSES =
			ConfigOptions.key("healthmonitor.resolver.classes").noDefaultValue();

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

	private volatile long jobStartExecutionTime = Long.MAX_VALUE;

	private volatile long successActionCount = 0;
	private volatile long failedActionCount = 0;

	private volatile boolean isEnabled;

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

		isEnabled = config.getBoolean(HEALTH_MONITOR_ENABLED);
		if (isEnabled) {
			loadPlugins();
		}

		long checkInterval = config.getLong(HEALTH_CHECK_INTERNAL);

		if (checkInterval > 0) {
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

	@VisibleForTesting
	public void loadPlugins() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		// try to close old plugins first.
		closePlugins();

		// load new plugins.
		loadDetectors();
		loadResolvers();
		loadActionSelector();
	}

	public void stop() {

		if (timedTaskHandler != null) {
			timedTaskHandler.cancel(true);
		}
		closePlugins();
	}

	@VisibleForTesting
	public void closePlugins() {

		if (this.actionSelector != null) {
			this.actionSelector.close();
			this.actionSelector = null;
		}

		if (detectors != null) {
			for (Detector detector : detectors) {
				detector.close();
			}
			detectors.clear();
			detectors = null;
		}

		if (resolvers != null) {
			for (Resolver resolver : resolvers) {
				resolver.close();
			}
			resolvers.clear();
			resolvers = null;
		}
	}

	private void loadActionSelector() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		this.actionSelector =
				(ActionSelector) Class.forName(config.getString(ACTION_SELECTOR_CLASS)).newInstance();
		LOGGER.info("Load action selector:" + actionSelector);
		this.actionSelector.open(this);
	}

	private void loadDetectors() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		Set<String> detectorClazzs = new HashSet<>();
		if (config.getString(DETECTOR_CLASSES) != null) {
			detectorClazzs.addAll(Arrays.asList(config.getString(DETECTOR_CLASSES).split(",")));
		} else {
			if (config.getBoolean(HealthMonitorOptions.ENABLE_PARALLELISM_RESCALE)) {
				// detector which will trigger rescale
				detectorClazzs.add(HighDelayDetector.class.getCanonicalName());
				detectorClazzs.add(DelayIncreasingDetector.class.getCanonicalName());
				detectorClazzs.add(OverParallelizedDetector.class.getCanonicalName());

				// detectors which will check state of job
				detectorClazzs.add(JobStableDetector.class.getCanonicalName());
				detectorClazzs.add(JobStuckDetector.class.getCanonicalName());
				detectorClazzs.add(FailoverDetector.class.getCanonicalName());
				detectorClazzs.add(FrequentFullGCDetector.class.getCanonicalName());
				detectorClazzs.add(LongTimeFullGCDetector.class.getCanonicalName());
			}
			if (config.getBoolean(HealthMonitorOptions.ENABLE_RESOURCE_RESCALE)) {
				detectorClazzs.add(HighCpuDetector.class.getCanonicalName());
				detectorClazzs.add(LowCpuDetector.class.getCanonicalName());
				detectorClazzs.add(HeapOOMDetector.class.getCanonicalName());
				detectorClazzs.add(FrequentFullGCDetector.class.getCanonicalName());
				detectorClazzs.add(LongTimeFullGCDetector.class.getCanonicalName());
				detectorClazzs.add(DirectOOMDetector.class.getCanonicalName());
				detectorClazzs.add(HighNativeMemoryDetector.class.getCanonicalName());
				detectorClazzs.add(KilledDueToMemoryExceedDetector.class.getCanonicalName());
				detectorClazzs.add(LowMemoryDetector.class.getCanonicalName());
				detectorClazzs.add(JobStableDetector.class.getCanonicalName());
			}
		}
		LOGGER.info("Load detectors:" + StringUtils.join(detectorClazzs, ","));
		this.detectors = new ArrayList<>(detectorClazzs.size());
		for (String clazz : detectorClazzs) {
			Detector detector = (Detector) Class.forName(clazz.trim()).newInstance();
			detectors.add(detector);

			detector.open(this);
		}

	}

	private void loadResolvers() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		Set<String> resolverClazzs = new HashSet<>();
		if (config.getString(RESOLVER_CLASSES) != null) {
			resolverClazzs.addAll(Arrays.asList(config.getString(RESOLVER_CLASSES).split(",")));
		} else {
			if (config.getBoolean(HealthMonitorOptions.ENABLE_PARALLELISM_RESCALE)) {
				resolverClazzs.add(ParallelismScaler.class.getCanonicalName());
			}
			if (config.getBoolean(HealthMonitorOptions.ENABLE_RESOURCE_RESCALE)) {
				resolverClazzs.add(CpuAdjuster.class.getCanonicalName());
				resolverClazzs.add(HeapMemoryAdjuster.class.getCanonicalName());
				resolverClazzs.add(DirectMemoryAdjuster.class.getCanonicalName());
				resolverClazzs.add(NativeMemoryAdjuster.class.getCanonicalName());
			}
		}
		LOGGER.info("Load resolvers:" + StringUtils.join(resolverClazzs, ","));
		this.resolvers = new ArrayList<>(resolverClazzs.size());
		for (String clazz : resolverClazzs) {
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

	public long getJobStartExecutionTime() {
		if (jobStartExecutionTime == Long.MAX_VALUE) {
			// check and set last start execution time.
			jobStartExecutionTime = MetricUtils.getStartExecuteTime(this);
		}
		return jobStartExecutionTime;
	}

	/**
	 * Health check for a job, which detects abnormal symptoms of job which detectors and tries to
	 * resolve abnormal status with registered Resolver.
	 */
	public class HealthChecker implements Runnable {

		@Override
		public void run() {
			try {
				check();
				// reset job config.
				jobConfig = null;
			} catch (Throwable e) {
				LOGGER.warn("Fail to check job status", e);
			}
		}

		public void check() {

			LOGGER.debug("Start to check job {}.", jobID);

			Configuration newConfig = getJobConfig().getConfig();
			if (isEnabled && !newConfig.getBoolean(HEALTH_MONITOR_ENABLED)) {
				LOGGER.info("Disabling health monitor");
				closePlugins();
				isEnabled = false;
				return;
			}

			if (!isEnabled && newConfig.getBoolean(HEALTH_MONITOR_ENABLED)) {
				try {
					LOGGER.info("Enabling health monitor");
					// reload configuration.
					for (String key : newConfig.keySet()) {
						config.setString(key , newConfig.getString(key, null));
					}
					loadPlugins();
					isEnabled = true;
				} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
					LOGGER.error("Fail to load plugins", e);
					return;
				}
			}

			if (!isEnabled) {
				LOGGER.debug("Health monitor disabled.");
				return;
			}

			List<Symptom> symptoms = new LinkedList<>();

			jobStartExecutionTime = Long.MAX_VALUE;

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

			}

		}
	}
}
