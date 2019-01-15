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

package org.apache.flink.runtime.healthmanager.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Health Manager Metric Group.
 */
public class HealthManagerMetricGroupTest {

	@Test
	public void testMetricGroup() {
		HealthManagerMetricGroup metricGroup = new HealthManagerMetricGroup(NoOpMetricRegistry.INSTANCE);
		assertEquals(new HashMap<String, String>(), metricGroup.getAllVariables());

		JobID jobID = new JobID();

		HealthMonitorMetricGroup jobMetricGroup = metricGroup.addJob(jobID, "test-1");
		assertFalse(jobMetricGroup.isClosed());

		assertEquals(new HashMap<String, String>(){{
			put(ScopeFormat.SCOPE_JOB_ID, jobID.toString());
			put(ScopeFormat.SCOPE_JOB_NAME, "test-1");
		}}, jobMetricGroup.getAllVariables());

		metricGroup.removeJob(jobID);
		assertTrue(jobMetricGroup.isClosed());
	}
}
