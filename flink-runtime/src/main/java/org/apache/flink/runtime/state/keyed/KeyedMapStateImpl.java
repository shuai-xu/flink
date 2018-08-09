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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.FieldBasedPartitioner;
import org.apache.flink.runtime.state.InternalColumnDescriptor;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link KeyedMapState} backed by an internal state.
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
	private static KeyedMapStateDescriptor stateDescriptor;

	/**
	 * Constructor with the internal state to store mappings.
	 *
	 * @param internalState The internal state where mappings are stored.
	 */
	public KeyedMapStateImpl(InternalState internalState) {
		super(internalState);
	}

	/**
	 * Creates and returns the descriptor for the internal state backing the
	 * keyed state.
	 *
	 * @param keyedStateDescriptor The descriptor for the keyed state.
	 * @param <K> Type of the keys in the state.
	 * @param <MK> Type of the map keys in the state.
	 * @param <MV> Type of the map values in the state.
	 * @return The descriptor for the internal state backing the keyed state.
	 */
	public static <K, MK, MV> InternalStateDescriptor createInternalStateDescriptor(
		final KeyedMapStateDescriptor<K, MK, MV> keyedStateDescriptor
	) {
		stateDescriptor = Preconditions.checkNotNull(keyedStateDescriptor);

		return new InternalStateDescriptorBuilder(keyedStateDescriptor.getName())
			.addKeyColumn("key", keyedStateDescriptor.getKeySerializer())
			.addKeyColumn("mapKey", keyedStateDescriptor.getMapKeySerializer())
			.addValueColumn("mapValue",
				keyedStateDescriptor.getMapValueSerializer(),
				keyedStateDescriptor.getMapValueMerger())
			.setPartitioner(new FieldBasedPartitioner(KEY_FIELD_INDEX, HashPartitioner.INSTANCE))
			.getDescriptor();
	}

	@Override
	Map<MK, MV> createMap() {
		return new HashMap<>();
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKey) throws Exception {
		InternalStateDescriptor descriptor = internalState.getDescriptor();
		InternalColumnDescriptor<K> keyDescriptor = (InternalColumnDescriptor<K>)descriptor.getKeyColumnDescriptor(KEY_FIELD_INDEX);
		K key = KvStateSerializer.deserializeValue(serializedKey, keyDescriptor.getSerializer());

		Map<MK, MV> map = get(key);
		if (map == null) {
			return null;
		}

		TypeSerializer<MK> mkSerializer = stateDescriptor.getMapKeySerializer();
		TypeSerializer<MV> mvSerializer = stateDescriptor.getMapValueSerializer();

		return KvStateSerializer.serializeMap(map.entrySet(), mkSerializer, mvSerializer);
	}
}
