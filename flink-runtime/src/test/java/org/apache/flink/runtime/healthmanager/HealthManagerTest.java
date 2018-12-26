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
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobStatus;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.LinkedList;

/**
 * Tests for Health Manager.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HealthManager.class, RestServerClientImpl.class, RestServerMetricProvider.class})
public class HealthManagerTest {

	@Test
	public void testStartStop() throws Exception {
		RestServerClientImpl mockRestClient = Mockito.mock(RestServerClientImpl.class);
		JobStatusMessage job1 = new JobStatusMessage(new JobID(), "1", JobStatus.RUNNING, 1);
		JobStatusMessage job2 = new JobStatusMessage(new JobID(), "2", JobStatus.RUNNING, 2);
		JobStatusMessage job1Finished = new JobStatusMessage(
				job1.getJobId(), job1.getJobName(), JobStatus.FINISHED, 2);

		Mockito.when(mockRestClient.listJob())
				.thenReturn(new LinkedList<>())
				.thenReturn(new LinkedList<JobStatusMessage>(){
					{
						add(job1);
						add(job2);
					}})
				.thenReturn(new LinkedList<JobStatusMessage>(){
					{
						add(job1Finished);
						add(job2);
					}});

		RestServerMetricProvider mockMetricProvider = Mockito.mock(RestServerMetricProvider.class);

		HealthMonitor mockMonitor = Mockito.mock(HealthMonitor.class);

		PowerMockito.whenNew(RestServerClientImpl.class).withAnyArguments().thenReturn(mockRestClient);
		PowerMockito.whenNew(RestServerMetricProvider.class).withAnyArguments().thenReturn(mockMetricProvider);
		PowerMockito.whenNew(HealthMonitor.class).withAnyArguments().thenReturn(mockMonitor);

		Configuration config = new Configuration();
		config.setLong("healthmanager.job.check.interval.ms", 100);
		String restURL  = "http://localhost:12345";

		HealthManager healthManager = new HealthManager(config, restURL);
		healthManager.start();
		Thread.sleep(1000);
		Mockito.verify(mockRestClient, Mockito.atLeast(3)).listJob();
		Mockito.verify(mockMonitor, Mockito.times(2)).start();
		Mockito.verify(mockMonitor, Mockito.times(1)).stop();

		healthManager.stop();
		Mockito.verify(mockMonitor, Mockito.times(2)).stop();

	}

}
