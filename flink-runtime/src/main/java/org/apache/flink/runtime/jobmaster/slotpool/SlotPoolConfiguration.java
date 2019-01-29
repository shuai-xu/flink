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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * A wrap of configurations for slot pool.
 */
public class SlotPoolConfiguration implements Serializable {

	/** The underlying configuration for the slot pool. */
	private final Configuration configuration;

	/** Whether to consider slot tags when tries to match slots with requests. */
	private boolean enableSlotTagMatching;

	/** Whether to converge slots in task managers. */
	private boolean enableSlotsConverging;

	// ------------------------------------------------------------------------

	public SlotPoolConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the underlying configuration.
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	public boolean isEnableSlotTagMatching() {
		return enableSlotTagMatching;
	}

	public void setEnableSlotTagMatching(boolean enableSlotTagMatching) {
		this.enableSlotTagMatching = enableSlotTagMatching;
	}

	public boolean isEnableSlotsConverging() {
		return enableSlotsConverging;
	}

	public void setEnableSlotsConverging(boolean enableSlotsConverging) {
		this.enableSlotsConverging = enableSlotsConverging;
	}
}
