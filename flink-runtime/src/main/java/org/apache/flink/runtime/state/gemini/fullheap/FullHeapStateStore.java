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

package org.apache.flink.runtime.state.gemini.fullheap;

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.gemini.AbstractStateStore;
import org.apache.flink.runtime.state.gemini.FullStateStoreSnapshot;
import org.apache.flink.runtime.state.gemini.RowMap;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.runtime.state.gemini.StateStoreSnapshot;

/**
 * Implementation of {@link AbstractStateStore} which stores states on heap
 * and takes full snapshots.
 */
public class FullHeapStateStore extends AbstractStateStore {

	public FullHeapStateStore(
		AbstractInternalStateBackend stateBackend,
		InternalStateDescriptor stateDescriptor
	) {
		super(stateBackend, stateDescriptor);
	}

	@Override
	protected RowMap createRowMap() {
		int numKeyColumns = stateDescriptor.getNumKeyColumns();
		Comparator[] columnComparators = new Comparator[numKeyColumns];
		for (int i = 0; i < numKeyColumns; i++) {
			columnComparators[i] =
				stateDescriptor.getKeyColumnDescriptor(i).getComparator();
		}

		RowMap dataRowMap = new CowHashRowMap(stateBackend, stateDescriptor);

		// We should build index in two cases:
		// 1. State has more than 1 key columns.
		// 2. State has only one key column and this column is sorted.
		if (numKeyColumns > 1 || columnComparators[0] != null) {
			return new CowHashRowMapWithIndex(numKeyColumns, columnComparators, dataRowMap);
		} else {
			return dataRowMap;
		}
	}

	@Override
	public StateStoreSnapshot createSnapshot(long checkpointId) {
		RowMapSnapshot rowMapSnapshot = rowMap.createSnapshot();
		return new FullStateStoreSnapshot(this, rowMapSnapshot);
	}

}
