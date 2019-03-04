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

package org.apache.flink.table.runtime.window.aligned;

import org.apache.flink.table.api.window.TimeWindow;

import java.io.Serializable;

/**
 * A {@link AlignedWindowTrigger} determines the when is the next trigger time and
 * which next window should be triggered.
 */
public interface AlignedWindowTrigger extends Serializable {

	/**
	 * Returns what's the next trigger time after this clock time.
	 * @param clockTime a clock time is the watermark or processing time
	 *                     which tracks the current time of the operator.
	 */
	long nextTriggerTime(long clockTime);

	/**
	 * Returns what's the next trigger window after this clock time.
	 * @param clockTime a clock time is the watermark or processing time
	 *                     which tracks the current time of the operator.
	 */
	TimeWindow nextTriggerWindow(long clockTime);
}
