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

import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * Interface for a map where the types of key and value are {@link Row}. The map doesn't
 * support null key and null value.
 */
public interface RowMap {

	/**
	 * Returns the total number of key-value mappings in this map.
	 *
	 * @return The number of key-value mappings in this map.
	 */
	int size();

	/**
	 * Returns whether this map is empty.
	 *
	 * @return {@code true} if this map has no mapping, {@code false} otherwise.
	 */
	boolean isEmpty();

	/**
	 * Returns the value to which the specified key is mapped.
	 *
	 * @param key The key. Not null.
	 * @return The value of the specified key, or {@code null} if no mapping is found.
	 */
	Row get(Row key);

	/**
	 * Associates the specified value with the specified key in this map. If the map
	 * previously contained a mapping for the key, the old value is replaced.
	 *
	 * @param key The key to add. Not null
	 * @param value The value to add.
	 * @return The previous value associated with the key, or {@code null} if there is no mapping.
	 */
	Row put(Row key, Row value);

	/**
	 * Merge the value into the already existed value associated with the key.
	 * If there is no value associated with the key before, the value will be added.
	 *
	 * @param key The key with which the value is associated.
	 * @param value The value to be merged.
	 * @return The previous value associated with the key, or null if there is no mapping.
	 */
	Row merge(Row key, Row value);

	/**
	 * Removes the mapping for the specified key from this map if present.
	 *
	 * @param key The key to remove.
	 * @return The previous value associated with the key, or {@code null} if there is no mapping.
	 */
	Row remove(Row key);

	/**
	 * Returns an iterator over all the rows in the map where heading
	 * keys are equal to the given prefix.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @return An iterator over all the rows in the map where heading
	 *         keys are equal to the given prefix.
	 */
	Iterator<Pair<Row, Row>> getIterator(Row prefixKeys);

	/**
	 * Returns an iterator over the rows where heading keys are equal to the given prefix
	 * and the succeeding key locates in the given range.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @param startKey The low endpoint (inclusive) of the keys succeeding the
	 *                 given prefix in the rows to be iterated over.
	 * @param endKey The high endpoint (exclusive) of the keys succeeding the
	 *               given prefix in the rows to be iterated over.
	 * @return An iterator where rows range from startRow(inclusive) to endRow(exclusive).
	 */
	<K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKeys, K startKey, K endKey);

	/**
	 * Returns the pair in the map whose key is smallest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair whose key is smallest among the pairs with the same prefix.
	 */
	Pair<Row, Row> firstPair(Row prefixKeys);

	/**
	 * Returns the pair in the map whose key is largest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair whose key is largest among the pairs with the same prefix.
	 */
	Pair<Row, Row> lastPair(Row prefixKeys);

	/**
	 * Takes a snapshot of the current map.
	 *
	 * @return Snapshot of this map.
	 */
	RowMapSnapshot createSnapshot();

	/**
	 * Releases the specified snapshot.
	 *
	 * @param snapshot The snapshot to release.
	 */
	void releaseSnapshot(RowMapSnapshot snapshot);
}
