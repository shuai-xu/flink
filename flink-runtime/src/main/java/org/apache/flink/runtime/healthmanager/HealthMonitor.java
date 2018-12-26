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
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.ActionSelector;
import org.apache.flink.runtime.healthmanager.plugins.Detector;
import org.apache.flink.runtime.healthmanager.plugins.DiagnoserAndResolver;
import org.apache.flink.runtime.healthmanager.plugins.Symptom;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Health Monitor which checks status of job periodically, and solve abnormal symptoms when detected.
 */
public class HealthMonitor {

	private static final ConfigOption<Long> HEALTH_CHECK_INTERNAL =
			ConfigOptions.key("healthmanager.health.check.interval.ms").defaultValue(60000L);

	private JobID jobID;
	private MetricProvider metricProvider;
	private RestServerClient restServerClient;
	private Configuration config;

	private ScheduledExecutorService executorService;

	private List<Detector> detectors;
	private List<DiagnoserAndResolver> diagnoserAndResolvers;
	private ActionSelector actionSelector;

	public HealthMonitor(
			JobID jobID,
			MetricProvider metricProvider,
			RestServerClient visitor,
			ScheduledExecutorService executorService,
			Configuration config) {

		this.jobID = jobID;
		this.executorService = executorService;
		this.metricProvider = metricProvider;
		this.restServerClient = visitor;
		this.config = config.clone();
	}

	public void start() {

		loadDetectors();
		loadDiagnosers();
		loadActionSelector();
		executorService.scheduleAtFixedRate(
				new HealthChecker(), 0, config.getLong(HEALTH_CHECK_INTERNAL), TimeUnit.MILLISECONDS);
	}

	public void stop() {
		executorService.shutdown();
	}

	private void loadActionSelector() {
		// TODO:: not implement yet!
	}

	private void loadDetectors() {
		// TODO:: not implement yet!
	}

	private void loadDiagnosers() {
		// TODO:: not implement yet!
	}

	/**
	 * Health check for a job, which detects abnormal symptoms of job which detectors and tries to
	 * resolve abnormal status with registered DiagnoserAndResolver.
	 */
	public class HealthChecker implements Runnable {

		@Override
		public void run() {

			List<Symptom> symptoms = new LinkedList<>();

			// 1. check abnormal symptoms.
			for (Detector detector: detectors) {
				Symptom symptom = detector.detect();
				if (symptom != null) {
					symptoms.add(symptom);
				}
			}

			// 2. diagnose and generate resolve action.
			List<Action> actions = new LinkedList<>();
			for (DiagnoserAndResolver diagnoserAndResolver : diagnoserAndResolvers) {
				if (diagnoserAndResolver.accept(symptoms)) {
					actions.add(diagnoserAndResolver.resolve(symptoms));
				}
			}

			if (actions.size() == 0) {
				return;
			}

			// 3. select an action to execute.
			Action action = actionSelector.accept(actions);

			if (action != null) {
				// 4. exccute an action.
				action.execute(restServerClient);

				// 5. validate result of action.
				if (!action.validate(metricProvider, restServerClient)) {

					// 6. execution roll back action if validation failed.
					action.rollback().execute(restServerClient);

					// 7. notify failure of an action.
					actionSelector.actionFailed(action);
				} else {
					// 6. notify success of an action.
					actionSelector.actionSucceed(action);
				}
			}

		}
	}
}
