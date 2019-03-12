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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.runtime.healthmanager.HealthMonitor.HEALTH_MONITOR_ENABLED;
import static org.junit.Assert.assertEquals;

/**
 * Tests for Health Monitor.
 */
public class HealthMonitorTest {

	@Test
	public void testDisableAndEnable() throws Exception {
		JobID jobID = new JobID();
		MetricProvider metricProvider = Mockito.mock(MetricProvider.class);
		RestServerClient restServerClient = Mockito.mock(RestServerClient.class);
		ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(
				1, new ExecutorThreadFactory("health-manager"));
		HealthMonitor healthMonitor = new HealthMonitor(jobID, metricProvider, restServerClient, executorService, new Configuration());
		HealthMonitor.HealthChecker healthChecker = healthMonitor.new HealthChecker();

		RestServerClient.JobConfig jobConfig = Mockito.mock(RestServerClient.JobConfig.class);
		Mockito.when(restServerClient.getJobConfig(Mockito.eq(jobID))).thenReturn(jobConfig);

		Configuration config1 = new Configuration();
		config1.setBoolean(HEALTH_MONITOR_ENABLED, false);
		Configuration config2 = new Configuration();
		Mockito.when(jobConfig.getConfig()).thenReturn(config1).thenReturn(config2);
		Whitebox.setInternalState(healthMonitor, "isEnabled", true);
		healthChecker.run();
		assertEquals(false, Whitebox.getInternalState(healthMonitor, "isEnabled"));
		assertEquals(null, Whitebox.getInternalState(healthMonitor, "jobConfig"));

		config2.setBoolean(HEALTH_MONITOR_ENABLED, true);
		config2.setBoolean("test-key", false);
		healthChecker.run();
		assertEquals(true, Whitebox.getInternalState(healthMonitor, "isEnabled"));
		assertEquals(false, healthMonitor.getConfig().getBoolean("test-key", true));
	}

}
