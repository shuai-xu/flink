/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

/**
 * Implementation of {@link org.apache.flink.runtime.state.heap.InternalKeyContext}.
 */
public class KeyContextImpl<K> implements InternalKeyContext {

	/** The serializer for the keys. */
	private final TypeSerializer<K> keySerializer;

	/** The number of key groups. */
	private final int numberOfKeyGroups;

	/** The key group assigned to this task. */
	private final KeyGroupRange keyGroupRange;

	/** The current key. */
	private K currentKey;

	public KeyContextImpl(
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange) {

		this.keySerializer = keySerializer;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;
	}

	/**
	 * Sets the current key that is used for partitioned state.
	 * @param newKey The new current key.
	 */
	public void setCurrentKey(K newKey) {
		this.currentKey = newKey;
	}

	@Override
	public K getCurrentKey() {
		return currentKey;
	}

	@Override
	public int getCurrentKeyGroupIndex() {
		return KeyGroupRangeAssignment.assignToKeyGroup(currentKey, numberOfKeyGroups);
	}

	@Override
	public int getNumberOfKeyGroups() {
		return numberOfKeyGroups;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Override
	public TypeSerializer getKeySerializer() {
		return keySerializer;
	}
}
