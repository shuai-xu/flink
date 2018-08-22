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

package org.apache.flink.streaming.api.bundle;

import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ScheduledFuture;

/**
 * A {@link BundleTrigger} that fires once the current system time passes the end time of a bundle, which is equal to
 * the start time of the bundle plus the given timeout.
 */
public class TimeBundleTrigger<T> implements BundleTrigger<T> {
	private static final long serialVersionUID = 7142843532861241200L;

	private final long timeout;
	private transient BundleTriggerCallback callback;
	private transient ProcessingTimeService timeService;
	private transient ScheduledFuture scheduledFuture;

	public TimeBundleTrigger(long timeout) {
		Preconditions.checkArgument(timeout > 0, "capacity must be greater than 0");
		this.timeout = timeout;
	}

	@Override
	public void registerBundleTriggerCallback(BundleTriggerCallback callback, BundleTriggerContext context) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		this.timeService = context.getProcessingTimeService();
		Preconditions.checkNotNull(timeService, "timeRegistry is null");
	}

	@Override
	public void onElement(T element) {
	}

	@Override
	public void reset() {
		if (scheduledFuture != null && !scheduledFuture.isDone() && !scheduledFuture.isCancelled()) {
			scheduledFuture.cancel(false);
			scheduledFuture = null;
		}
		if (timeService.isTerminated()) {
			throw new IllegalStateException("ProcessingTimeRegistry is terminated.");
		}
		long timestamp = timeService.getCurrentProcessingTime() + timeout;
		scheduledFuture = timeService.registerTimer(timestamp, new ProcessingTimeCallback() {
			@Override
			public void onProcessingTime(long timestamp) throws Exception {
				callback.finishBundle();
			}
		});
	}

	@Override
	public String explain() {
		return "TimeBundleTrigger with timeout is " + timeout + " ms";
	}

}
