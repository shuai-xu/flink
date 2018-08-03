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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * A helper class to iterator over all entries in a nested map with the given
 * prefix keys and a range of the key succeeding the prefix keys.
 */
final class RangeHeapIterator extends AbstractHeapIterator {

	/**
	 * The low endpoint of the first suffix key. Null if there is no limit on
	 * the lower bound of the fist suffix key.
	 */
	private final Object headSuffixKeyStart;

	/**
	 * The high endpoint of the first suffix key. Null if there is no limit on
	 * the higher bound of the first suffix key.
	 */
	private final Object headSuffixKeyEnd;

	/**
	 * Constructor with given map, the number of keys, the values of prefix keys
	 * and the range of the first suffix key.
	 *
	 * @param rootMap The map to be iterated over.
	 * @param numKeys The number of keys in the map.
	 * @param prefixKeys The values of the prefix keys.
	 * @param headSuffixKeyStart The low endpoint of the first suffix key.
	 * @param headSuffixKeyEnd The high endpoint of the first suffix key.
	 */
	RangeHeapIterator(
		Map rootMap,
		int numKeys,
		Row prefixKeys,
		Object headSuffixKeyStart,
		Object headSuffixKeyEnd
	) {
		super(rootMap, numKeys, prefixKeys);

		Preconditions.checkArgument(
			headSuffixKeyStart != null || headSuffixKeyEnd != null);

		this.headSuffixKeyStart = headSuffixKeyStart;
		this.headSuffixKeyEnd = headSuffixKeyEnd;

		initialize();
	}

	@SuppressWarnings("unchecked")
	@Override
	Iterator buildSuffixIterator(int index, Map map) {
		if (index == 0) {
			Preconditions.checkState(map instanceof SortedMap);
			SortedMap sortedMap = (SortedMap) map;

			if (headSuffixKeyStart == null) {
				return sortedMap.headMap(headSuffixKeyEnd)
					.entrySet().iterator();
			} else if (headSuffixKeyEnd == null) {
				return sortedMap.tailMap(headSuffixKeyStart)
					.entrySet().iterator();
			} else {
				return sortedMap.subMap(headSuffixKeyStart, headSuffixKeyEnd)
					.entrySet().iterator();
			}
		} else {
			return map.entrySet().iterator();
		}
	}
}
