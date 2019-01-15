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
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.groups.ComponentMetricGroup;

import java.util.HashMap;
import java.util.Map;

/**
 * Metric Group For Health Manager.
 */
public class HealthManagerMetricGroup extends ComponentMetricGroup<HealthManagerMetricGroup> {

	private Map<JobID, HealthMonitorMetricGroup> jobMetrics = new HashMap<>();

	/**
	 * Creates a new ComponentMetricGroup.
	 *
	 * @param registry registry to register new metrics with
	 */
	public HealthManagerMetricGroup(MetricRegistry registry) {
		super(registry, new String[0], null);
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return jobMetrics.values();
	}

	@Override
	protected String getGroupName(CharacterFilter filter) {
		return "healthmanager";
	}

	@Override
	protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		// use job manager query scope since health manager inited in job manager.
		return new QueryScopeInfo.JobManagerQueryScopeInfo();
	}

	/**
	 * Add a new job and create an HealthMonitorMetricGroup for the new job.
	 * @param jobId    id of the new job.
	 * @param jobName  name of the new job.
	 * @return
	 */
	public HealthMonitorMetricGroup addJob(JobID jobId, String jobName) {
		return jobMetrics.computeIfAbsent(
				jobId,
				k -> new HealthMonitorMetricGroup(
						registry,
						this,
						jobId,
						jobName,
						new String[]{jobId.toString(), jobName}));
	}

	/**
	 * Remove a job and destroy the HealthMonitorMetricGroup of the job.
	 * @param jobID   is of the job.
	 */
	public void removeJob(JobID jobID) {
		jobMetrics.remove(jobID).close();
	}
}
