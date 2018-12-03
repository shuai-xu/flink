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

import java.util.Iterator;

/**
 * Interface of iterator for state storage.
 */
public interface StorageIterator<K, V> extends Iterator<Pair<K, V>>, AutoCloseable {

	/** Get the key of current item.*/
	K key();

	/** Return the value of current item.*/
	V value();

	/**
	 * Seek to the specified key.
	 * If the specified key does not exist, after this operation the cursor will
	 * stop at the key less than the specified key.*/
	void seek(K key);

	/** Seek to the first item of current iterator. */
	void seekToFirst();

	/** Seek to the last item of current iterator. */
	void seekToLast();
}
