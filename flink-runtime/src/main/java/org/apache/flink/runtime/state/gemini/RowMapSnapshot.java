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
 * Interface of snapshot for {@link RowMap}.
 */
public interface RowMapSnapshot {

	/**
	 * Returns the total number of key-value mappings in this snapshot.
	 *
	 * @return The number of key-value mappings in this snapshot.
	 */
	int size();

	/**
	 * Returns an iterator over the snapshot on the given group.
	 *
	 * @param group The group to iterate.
	 * @return The iterator over the snapshot on the given group.
	 */
	Iterator<Pair<Row, Row>> groupIterator(int group);

	/**
	 * Releases the snapshot.
	 */
	void releaseSnapshot();
}
