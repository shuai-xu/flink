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

package org.apache.flink.runtime.state;

import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface for internal states.
 *
 * <p>Each internal state contains a set of rows which are composed of a set of
 * columns. All rows in the same state have the same set of columns. Columns are
 * typed and have unique names in the same state.
 *
 * <p>A sequence of columns is used to form the primary key of the row. The
 * primary keys are unique within the state. The rows are partitioned into a
 * set of groups according to their primary keys. The total number of
 * groups is determined by the maximum parallelism of the operator to which
 * the state belongs. By default, the rows are hash-partitioned. Users can
 * change the grouping scheme by providing a customized partition method in
 * the state's descriptor.
 *
 * <p>An internal state can be either global or local. For global states, the
 * state viewed by different tasks are identical and each task will be assigned
 * roughly same number of groups. When the number of tasks is changed, the
 * groups of global states remain the same and will be re-distributed to new
 * tasks. For local states, each task will obtain an individual and complete
 * view of the state. Primary keys are unique within each task, but may appear
 * on different tasks. When the number of tasks changes, the groups of old
 * tasks will be split and merged to construct the groups viewed by new tasks.
 */
public interface InternalState {

	/**
	 * Returns the descriptor of the state.
	 *
	 * @return The descriptor of the state.
	 */
	InternalStateDescriptor getDescriptor();

	/**
	 * Returns the total number of groups in the state.
	 *
	 * @return The total number of groups in the state.
	 */
	int getNumGroups();

	/**
	 * Returns the set of groups in the state partition.
	 *
	 * @return The set of groups in the state partition.
	 */
	GroupSet getPartitionGroups();

	/**
	 * Returns the value associated with the given key in the state. Null will
	 * be returned if this state contains no row for the key.
	 *
	 * @param key The key of the row to be retrieved.
	 * @return The value associated with the given key.
	 */
	@Nullable
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
	 * Merge the given value to exist value of the given key. If the state didn't
	 * previously contains a row for the given key, the result will be the given value.
	 *
	 * @param key The key with which the given value is to be merged to.
	 * @param value The value to be merged to the given key.
	 */
	void merge(Row key, Row value);

	/**
	 * Removes the row for the given key from the state if it is present.
	 *
	 * @param key The key of the row to be removed.
	 */
	void remove(Row key);

	/**
	 * Returns all values associated with the given keys in the state (optional
	 * operation).
	 *
	 * @param keys The keys of the rows to be retrieved.
	 * @return The values associated with the given keys.
	 */
	Map<Row, Row> getAll(Collection<Row> keys);

	/**
	 * Adds all the mappings in the given map into the state (optional
	 * operation). The addition of the mappings is atomic, exceptions will be
	 * thrown if some of them fail to be added.
	 *
	 * @param pairs The pairs to be added into the state.
	 */
	void putAll(Map<Row, Row> pairs);

	/**
	 * Merge all the mappings in the given map into the state.
	 * The addition of the mappings is atomic, exceptions will be thrown
	 * if some of them fail to be merged.
	 *
	 * @param pairs The pairs to be merged into the state.
	 */
	void mergeAll(Map<Row, Row> pairs);

	/**
	 * Removes all the rows whose keys appear in the given collection from the
	 * state (optional operation). The removal of these pairs are atomic,
	 * exceptions will be thrown if some of the pairs fail to be removed.
	 *
	 * @param keys The keys of the rows to be removed.
	 */
	void removeAll(Collection<Row> keys);

	/**
	 * Returns an iterator over all the rows in the state. There are no
	 * guarantees concerning the order in which the rows are iterated unless
	 * the columns are ordered.
	 *
	 * @return An iterator over all the rows in the state.
	 */
	Iterator<Pair<Row, Row>> iterator();

	/**
	 * Returns an iterator over all the rows in the state where heading
	 * keys are equal to the given prefix.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @return An iterator over all the rows in the state where heading
	 *         keys are equal to the given prefix.
	 */
	Iterator<Pair<Row, Row>> prefixIterator(Row prefixKeys);

	/**
	 * Returns the pair in the state whose key is smallest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair in the state whose key is smallest among the pairs
	 * with the same prefix.
	 */
	Pair<Row, Row> firstPair(Row prefixKeys);

	/**
	 * Returns the pair in the state whose key is largest among the pairs
	 * with the given prefix. If the keys succeeding the prefix are not sorted,
	 * then the result is non-deterministic.
	 *
	 * @param prefixKeys The heading keys of the row to be retrieved.
	 * @return The pair in the given group whose key is largest among the pairs
	 * with the same prefix.
	 */
	Pair<Row, Row> lastPair(Row prefixKeys);

	/**
	 * Returns an iterator over the rows in the state where heading
	 * keys are equal to the given prefix and the succeeding key is strictly
	 * less than {@code endKey}.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @param endKey The high endpoint (exclusive) of the keys succeeding the
	 *               given prefix in the rows to be iterated over.
	 * @param <K> Type of the keys succeeding the prefix
	 * @return An iterator over the rows in the state where heading
	 *         keys are equal to the given prefix and the succeeding key is
	 *         strictly less than {@code endKey}.
	 */
	<K> Iterator<Pair<Row, Row>> headIterator(Row prefixKeys, K endKey);

	/**
	 * Returns an iterator over the rows in the state where heading
	 * keys are equal to the given prefix and the succeeding key is equal to or
	 * greater than {@code startKey}.
	 *
	 * @param prefixKeys The heading keys of the rows to be iterated over.
	 * @param startKey The low endpoint (inclusive) of the keys succeeding the
	 *                 given prefix in the rows to be iterated over.
	 * @param <K> Type of the keys succeeding the prefix.
	 * @return An iterator over the rows in the state where heading
	 *         keys are equal to the given prefix and whose the succeeding key
	 *         is equal to or greater than {@code startKey}.
	 */
	<K> Iterator<Pair<Row, Row>> tailIterator(Row prefixKeys, K startKey);

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
	 * @param <K> Type of the keys succeeding the prefix.
	 * @return An iterator over the rows in the given group where heading
	 *         keys are equal to the given prefix and the succeeding key locates
	 *         in the given range.
	 */
	<K> Iterator<Pair<Row, Row>> subIterator(Row prefixKeys, K startKey, K endKey);
}
