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

package org.apache.flink.table.runtime.window.aligned;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.runtime.functions.ExecutionContext;
import org.apache.flink.util.Collector;

/**
 * A {@link AlignedWindowAggregator} is used to access & maintain window states and fire window
 * results. It can be a buffered or memory-managed-buffered implementation to reduce state access.
 * @param <K> type of key
 * @param <W> type of window
 * @param <IN> type of input element
 */
public interface AlignedWindowAggregator<K, W, IN> extends Function {

	/**
	 * Initialization method for the function. It is called before the actual working methods,
	 * e.g. {@link #addElement(Object, Object, Object)}.
	 */
	void open(ExecutionContext ctx) throws Exception;

	/**
	 * Adds the input element under the given key and window.
	 */
	void addElement(K key, W window, IN input) throws Exception;

	/**
	 * A callback to fire window results for all the keys under the given window.
	 */
	void fireWindow(W window, Collector<K> out) throws Exception;

	/**
	 * A callback to expire the given window. All the state under the window can be removed.
	 */
	void expireWindow(TimeWindow window) throws Exception;

	/**
	 * Snapshot all the buffered data to state.
	 */
	void snapshot() throws Exception;

	/**
	 * Returns the lowest window in the state.
	 */
	TimeWindow lowestWindow() throws Exception;

	/**
	 * Returns the ascending window iterable in the state.
	 */
	Iterable<TimeWindow> ascendingWindows() throws Exception;

	/**
	 * Returns a view of the windows who are greater than or equal to the {@code fromWindow}.
	 */
	Iterable<TimeWindow> ascendingWindows(TimeWindow fromWindow) throws Exception;

	/**
	 * Tear-down method for the function. It is called after the last call
	 * to the main working methods
	 */
	void close() throws Exception;
}
