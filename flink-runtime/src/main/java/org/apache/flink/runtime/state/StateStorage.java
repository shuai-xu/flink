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

package org.apache.flink.runtime.state;

import java.util.Map;

/**
 * Interface for State Storage, State storage is a Key/Value store that
 * stores all the states.
 *
 * @param <K> Type of key that will be stored to the current storage.
 * @param <V> Type of value that will be stored to the current storage.
 */
public interface StateStorage<K, V> {

	/**
	 * Associates the given value with the given key in the storage.
	 * If the storage previously contains a value for the given key,
	 * the old value will be replaced with the new value.
	 *
	 * @param key The key with which the given value is to be associated.
	 * @param value The value to be associated with the given key.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void put(K key, V value) throws Exception;

	/**
	 * Returns the value associated with the given key in the state.
	 * Null will be returned if this state contains no value for the key.
	 *
	 * @param key The key of the value to be retrieved.
	 * @return The value associated with the given key.
	 *
	 * @throws Exception Thrown if the system cannot access the value from storage.
	 */
	V get(K key) throws Exception;

	/**
	 * Removes the value for the given key from the storage if it is present.
	 *
	 * @param key The key of the value to be removed.
	 * @return <tt>true</tt> if remove successfully.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	boolean remove(K key) throws Exception;

	/**
	 * Returns an iterator over all the key-values in the storage. There are no
	 * guarantees concerning the order in which the key-values are iterated.
	 *
	 * @return An iterator over all the key-values in the state.
	 *
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	StorageIterator iterator() throws Exception;

	/**
	 * Merge the given value to exist value of the given key. If the state didn't
	 * previously contains a value for the given key, the result will be the given value.
	 *
	 * @param key The key with which the given value is to be merged to.
	 * @param value The value to be merged to the given key.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void merge(K key, V value) throws Exception;

	/**
	 * Adds all the mappings in the given map into the storage (optional
	 * operation). The addition of the mappings is atomic, exceptions will be
	 * thrown if some of them fail to be added.
	 *
	 * @param pairs The pairs to be added into the state.
	 *
	 * @throws Exception The method may forward exception thrown internally (by I/O or functions).
	 */
	void putAll(Map<K, V> pairs) throws Exception;

	/**
	 * Returns whether the key/value need to be (de)serialized when put to the storage,
	 * or get from the storage.
	 *
	 * @return <tt>true</tt> for need (de)serialize before put to/get from the storage.
	 */
	boolean lazySerde();

	/**
	 * Whether the storage support multi column family.
	 *
	 * @return <tt>true</tt> for the storage supports multi column family.
	 */
	boolean supportMultiColumnFamilies();

	/**
	 * Returns the StorageInstance associated with the storage.
	 *
	 * @return The storageInstance associated with the storage.
	 */
	StorageInstance getStorageInstance();
}
