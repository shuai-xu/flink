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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.healthmanager.HealthMonitor;
import org.apache.flink.runtime.healthmanager.plugins.Action;
import org.apache.flink.runtime.healthmanager.plugins.ActionSelector;

import java.util.List;

/**
 * First Valid Action Selector select first action not in the blacklist.
 */
public class FirstValidActionSelector implements ActionSelector {

	private static final ConfigOption<Long> ACTION_BLACK_LIST_INTERVAL =
			ConfigOptions.key("action.selector.blacklist.interval.ms")
			.defaultValue(300_000L);

	private Action lastFailedAction;
	private long lastFailedTime;
	private long blacklistThreshold;

	@Override
	public void open(HealthMonitor monitor) {
		blacklistThreshold = monitor.getConfig().getLong(ACTION_BLACK_LIST_INTERVAL);
	}

	@Override
	public void stop() {

	}

	@Override
	public Action accept(List<Action> actions) {
		for (Action action: actions) {
			if (lastFailedAction != null && action.getClass().equals(lastFailedAction.getClass()) &&
					System.currentTimeMillis() - lastFailedTime <  blacklistThreshold) {
				continue;
			}
			return action;
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
