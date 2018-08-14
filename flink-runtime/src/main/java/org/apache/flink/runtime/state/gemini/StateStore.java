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
 * Storage for state. Key-value mappings of a state are put into one {@link StateStore}.
 */
public interface StateStore {

	/**
	 * Returns the value associated with the given key in the state. Null will
	 * be returned if this state contains no row for the key.
	 *
	 * @param key The key of the row to be retrieved.
	 * @return The value associated with the given key.
	 */
	Row get(Row key);

	/**
	 * Associates the given value with the given key in the state. If the state
	 * previously contains a row for the given key, the old value will be
	 * replaced with the new value.
	 *
	 * @param key The key with which the given value is to be associated.
	 * @param value The value to be associated with the given key.
	 */
	void put(Row key, Row value);

	/**
	 * Merge the value into the already existed value associated with the key.
	 * If there is no value associated with the key, the value will be added.
	 *
	 * @param key The key with which the value is associated.
	 * @param value The value to be merged.
	 */
	void merge(Row key, Row value);

	/**
	 * Removes the row for the given key from the state if it is present.
	 *
	 * @param key The key of the row to be removed.
	 */
	void remove(Row key);

	/**
	 * Returns an iterator over all the rows in the state where heading
	 * keys are equal to the given prefix.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @return An iterator over all the rows in the state where heading
	 *         keys are equal to the given prefix.
	 */
	Iterator<Pair<Row, Row>> getIterator(Row prefixKeys);

	/**
	 * Returns an iterator over the rows in the given group where heading
	 * keys are equal to the given prefix and the succeeding key locates in the
	 * given range.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @param startKey The low endpoint (inclusive) of the keys succeeding the
	 *                 given prefix in the rows to be iterated over.
	 * @param endKey The high endpoint (exclusive) of the keys succeeding the
	 *               given prefix in the rows to be iterated over.
	 * @return An iterator over the rows in the given group where heading
	 *         keys are equal to the given prefix and the succeeding key locates
	 *         in the given range.
	 */
	<K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKeys, K startKey, K endKey);

	/**
	 * Returns the pair in the state whose key is smallest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair in the state whose key is smallest among the pairs
	 *			with the same prefix.
	 */
	Pair<Row, Row> firstPair(Row prefixKeys);

	/**
	 * Returns the pair in the state whose key is largest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair in the given group whose key is largest among the pairs
	 *			with the same prefix.
	 */
	Pair<Row, Row> lastPair(Row prefixKeys);

	/**
	 * Creates a snapshot of the current state.
	 *
	 * @param checkpointId The checkpoint id of this snapshot.
	 * @return A snapshot of the current state.
	 */
	StateStoreSnapshot createSnapshot(long checkpointId);

	/**
	 * Releases the specified snapshot.
	 *
	 * @param snapshot The snapshot to release.
	 */
	void releaseSnapshot(StateStoreSnapshot snapshot);
}
