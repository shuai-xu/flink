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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.healthmanager.plugins.utils.MetricUtils;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyVararg;

/**
 * Base class for detector test.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricUtils.class)
public abstract class DetectorTestBase {
	protected JobID jobID;
	protected HealthMonitor monitor;
	protected Configuration config;
	protected RestServerClient.JobConfig jobConfig;
	protected RestServerClient restClient;
	protected MetricProvider metricProvider;

	@Before
	public void setup() {
		monitor = Mockito.mock(HealthMonitor.class);
		jobID = new JobID();
		Mockito.when(monitor.getJobID()).thenReturn(jobID);
		config = new Configuration();
		Mockito.when(monitor.getConfig()).thenReturn(config);
		jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(monitor.getJobConfig()).thenReturn(jobConfig);
		restClient = Mockito.mock(RestServerClient.class);
		Mockito.when(monitor.getRestServerClient()).thenReturn(restClient);
		metricProvider = Mockito.mock(MetricProvider.class);
		Mockito.when(monitor.getMetricProvider()).thenReturn(metricProvider);

		PowerMockito.mockStatic(MetricUtils.class);
		Mockito.when(MetricUtils.validateTaskMetric(Mockito.any(HealthMonitor.class), anyLong(), anyVararg())).thenReturn(true);

	}
}
