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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;

public class ExternalBlockResultPartitionManager implements ResultPartitionProvider {

	public ExternalBlockResultPartitionManager(
		ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration) {
		// TODO
	}

	/**
	 * This method is used to create a mapping between yarnAppId and user.
	 */
	public void initializeApplication(String user, String appId) {
		// TODO
	}

	/**
	 * This method is used to remove both in-memory meta info and local files when
	 * this application is stopped in Yarn.
	 */
	public void stopApplication(String appId) {
		// TODO
	}

	public void stop() {
		// TODO
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
		ResultPartitionID partitionId,
		int index,
		BufferAvailabilityListener availabilityListener) throws IOException {

		// TODO
		return null;
	}
}
