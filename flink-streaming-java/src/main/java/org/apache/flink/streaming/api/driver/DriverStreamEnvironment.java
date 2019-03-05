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

package org.apache.flink.streaming.api.driver;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DriverConfigConstants;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.net.URL;

/**
 * DriverStreamEnvironment is a sub-class of RemoteStreamEnvironment. It keeps a driver id and sets user configured
 * settings before submit or execute user job.
 */
public class DriverStreamEnvironment extends RemoteStreamEnvironment {

	private final String driverId;

	public DriverStreamEnvironment(String host, int port, String driverId, String[] jarFiles, URL[] globalClasspaths, Configuration configuration) {
		super(host, port, configuration, jarFiles, globalClasspaths);
		this.driverId = driverId;
	}

	public void setAsContext() {
		StreamExecutionEnvironmentFactory factory = () -> this;
		StreamExecutionEnvironment.initializeContextEnvironment(factory);
	}

	public void resetContextEnvironments() {
		StreamExecutionEnvironment.resetContextEnvironment();
	}

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		SavepointRestoreSettings savepointRestoreSettings = getJobSavePointSettingsFromConfiguration();
		return executeInternal(jobName, false, savepointRestoreSettings).getJobExecutionResult();
	}

	@Override
	public JobSubmissionResult submit(String jobName) throws Exception {
		SavepointRestoreSettings savepointRestoreSettings = getJobSavePointSettingsFromConfiguration();
		return executeInternal(jobName, true, savepointRestoreSettings);
	}

	@Override
	protected JobSubmissionResult executeInternal(String jobName, boolean detached, SavepointRestoreSettings savepointRestoreSettings) throws ProgramInvocationException {
		StreamGraph streamGraph = getStreamGraph();
		String newJobName = driverId + "_" + jobName;
		streamGraph.setJobName(newJobName);
		transformations.clear();
		// ignore detached parameter
		JobSubmissionResult submissionResult = executeRemotely(streamGraph, jarFiles, detached, savepointRestoreSettings);
		return submissionResult;
		//return this.detached ? DetachedEnvironment.DetachedJobExecutionResult.INSTANCE : submissionResult;
	}

	private SavepointRestoreSettings getJobSavePointSettingsFromConfiguration() {
		Configuration clientConfiguration = getClientConfiguration();
		String savepointRestorePath = clientConfiguration.getString(DriverConfigConstants.FLINK_DRIVER_SAVEPOINT_RESTORE_SETTINGS_PATH, null);
		boolean allowNonRestoredState = clientConfiguration.getBoolean(DriverConfigConstants.FLINK_DRIVER_SAVEPOINT_RESTORE_SETTINGS_ALLOWNONRESTORESTATE, false);
		boolean resumeFromLatestCheckpoint = clientConfiguration.getBoolean(DriverConfigConstants.FLINK_DRIVER_SAVEPOINT_RESTORE_SETTINGS_RESUMEFROMLATESTCHECKPOINT, false);
		if (savepointRestorePath == null) {
			return SavepointRestoreSettings.none();
		}
		if (resumeFromLatestCheckpoint) {
			return SavepointRestoreSettings.forResumePath(savepointRestorePath, allowNonRestoredState);
		} else {
			return SavepointRestoreSettings.forPath(savepointRestorePath, allowNonRestoredState);
		}

	}

}
