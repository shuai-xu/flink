/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.gemini.fullheap;

import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * Interface for prefix key index.
 */
public interface CowPrefixKeyIndex {

	/**
	 * Adds the index for the key.
	 *
	 * @param key The key to add.
	 */
	void addKey(Row key);

	/**
	 * Removes the index for the key.
	 *
	 * @param key The key to remove.
	 */
	void removeKey(Row key);

	/**
	 * Returns the smallest key with the given prefix.
	 *
	 * @param prefixKey The prefix key.
	 * @return The smallest key with the given prefix.
	 */
	Row firstRowKey(Row prefixKey);

	/**
	 * Returns the largest key with the given prefix.
	 *
	 * @param prefixKey The prefix key.
	 * @return The largest key with the given prefix.
	 */
	Row lastRowKey(Row prefixKey);

	/**
	 * Returns an iterator over the keys where heading keys are equal to the given prefix
	 * and the succeeding key locates in the given range.
	 *
	 * @param prefixKey The heading keys of the rows to be iterated over.
	 * @param startKey The low endpoint (inclusive) of the keys succeeding the given prefix.
	 * @param endKey The high endpoint (exclusive) of the keys succeeding the given prefix.
	 * @param <K> Type of the keys succeeding the prefix.
	 * @return An iterator over the keys where heading keys are equal to the given prefix
	 * 			and the succeeding key locates in the given range.
	 */
	<K> Iterator<Row> getSubIterator(Row prefixKey, K startKey, K endKey);

}
