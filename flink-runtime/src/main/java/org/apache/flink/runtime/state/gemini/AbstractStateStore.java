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

import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;

/**
 * The base implementation of {@link StateStore}.
 */
public abstract class AbstractStateStore implements StateStore {

	protected final AbstractInternalStateBackend stateBackend;

	protected final InternalStateDescriptor stateDescriptor;

	protected final RowMap rowMap;

	public AbstractStateStore(
		AbstractInternalStateBackend stateBackend,
		InternalStateDescriptor stateDescriptor) {
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
		this.stateDescriptor = Preconditions.checkNotNull(stateDescriptor);

		this.rowMap = createRowMap();
	}

	/**
	 * Creates a {@link RowMap} for this store.
	 *
	 * @return A {@link RowMap} for this store.
	 */
	protected abstract RowMap createRowMap();

	@Override
	public Row get(Row key) {
		return rowMap.get(key);
	}

	@Override
	public void put(Row key, Row value) {
		rowMap.put(key, value);
	}

	@Override
	public void remove(Row key) {
		rowMap.remove(key);
	}

	@Override
	public Iterator<Pair<Row, Row>> getIterator(Row prefixKeys) {
		return rowMap.getIterator(prefixKeys);
	}

	@Override
	public <K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKeys, K startKey, K endKey) {
		return rowMap.getSubIterator(prefixKeys, startKey, endKey);
	}

	@Override
	public Pair<Row, Row> firstPair(Row prefixKeys) {
		return rowMap.firstPair(prefixKeys);
	}

	@Override
	public Pair<Row, Row> lastPair(Row prefixKeys) {
		return rowMap.lastPair(prefixKeys);
	}

	@Override
	public void releaseSnapshot(StateStoreSnapshot snapshot) {
		// nothing to do
	}

}
