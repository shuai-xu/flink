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

package org.apache.flink.runtime.healthmanager.plugins.actionselectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.ActionSelector;
import org.apache.flink.runtime.healthmanager.plugins.actions.AdjustJobResource;
import org.apache.flink.runtime.healthmanager.plugins.actions.RescaleJobParallelism;

import java.util.List;

/**
 * RescaleResourcePriorActionSelector prioritize combing and selecting AdjustJobResource actions
 * than RescaleJobParallelism actions.
 */
public class RescaleResourcePriorActionSelector implements ActionSelector {

	private static final ConfigOption<Long> ACTION_BLACK_LIST_INTERVAL =
		ConfigOptions.key("action.selector.blacklist.interval.ms")
			.defaultValue(300_000L);

	private Action lastFailedAction;
	private long lastFailedTime;
	private long blacklistThreshold;
	private JobID jobId;

	@Override
	public void open(HealthMonitor monitor) {
		blacklistThreshold = monitor.getConfig().getLong(ACTION_BLACK_LIST_INTERVAL);
		jobId = monitor.getJobID();
	}

	@Override
	public void close() {

	}

	@Override
	public Action accept(List<Action> actions) {
		AdjustJobResource adjustJobResource = null;
		RescaleJobParallelism rescaleJobParallelism = null;
		for (Action action: actions) {
			if (action instanceof AdjustJobResource) {
				if (adjustJobResource == null) {
					adjustJobResource = (AdjustJobResource) action;
				} else {
					adjustJobResource = ((AdjustJobResource) action).merge(adjustJobResource);
				}
				continue;
			}

			if (action instanceof  RescaleJobParallelism) {
				rescaleJobParallelism = (RescaleJobParallelism) action;
			}
		}

		if (adjustJobResource != null && adjustJobResource.getActionMode() == Action.ActionMode.IMMEDIATE) {
			return adjustJobResource;
		}

		if (rescaleJobParallelism != null && rescaleJobParallelism.getActionMode() == Action.ActionMode.IMMEDIATE){
			return rescaleJobParallelism;
		}

		return null;
	}

	@Override
	public void actionFailed(Action action) {
		lastFailedAction = action;
		lastFailedTime = System.currentTimeMillis();
	}

	@Override
	public void actionSucceed(Action action) {
	}
}
