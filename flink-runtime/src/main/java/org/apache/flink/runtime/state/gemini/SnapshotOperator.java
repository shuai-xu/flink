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

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StatePartitionSnapshot;

/**
 * Interface for a snapshot operator which is responsible to make a snapshot.
 */
public interface SnapshotOperator {

	/**
	 * Takes a snapshot in synchronous part of a checkpoint.
	 */
	void takeSnapshot();

	/**
	 * Materializes the snapshot in asynchronous part of a checkpoint.
	 */
	SnapshotResult<StatePartitionSnapshot> materializeSnapshot() throws Exception;

	/**
	 * Releases resources of this snapshot.
	 *
	 * @param cancelled {@code true} if this snapshot is cancelled, otherwise {@code false};
	 */
	void releaseResources(boolean cancelled);

}
