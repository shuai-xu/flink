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

package org.apache.flink.runtime.state3.keyed;

import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link KeyedMapState} backed by a state storage.
 *
 * @param <K> Type of the keys in the state.
 * @param <MK> Type of the map keys in the state.
 * @param <MV> Type of the map values in the state.
 */
public final class KeyedMapStateImpl<K, MK, MV>
	extends AbstractKeyedMapStateImpl<K, MK, MV, Map<MK, MV>>
	implements KeyedMapState<K, MK, MV> {

	/**
	 * The descriptor of current state.
	 */
	private KeyedMapStateDescriptor stateDescriptor;

	/**
	 * Constructor with the state storage to store mappings.
	 *
	 * @param stateStorage The state storage where mappings are stored.
	 */
	public KeyedMapStateImpl(KeyedMapStateDescriptor descriptor, StateStorage stateStorage) {
		super(stateStorage);

		this.stateDescriptor = Preconditions.checkNotNull(descriptor);
	}

	@Override
	public KeyedMapStateDescriptor getDescriptor() {
		return stateDescriptor;
	}

	@Override
	Map<MK, MV> createMap() {
		return new HashMap<>();
	}

}
