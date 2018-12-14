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

package org.apache.flink.contrib.streaming.state3;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.DefaultStatePartitionSnapshot;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state3.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state3.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Full restore operation for RocksDB InternalStateBackend.
 */
public class RocksDBFullRestoreOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBFullRestoreOperation.class);

	/** The state backend who wants to restore the snapshot.*/
	private final RocksDBInternalStateBackend stateBackend;

	private final Map<Integer, String> id2StateName = new HashMap<>();

	RocksDBFullRestoreOperation(RocksDBInternalStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public void restore(
		Collection<StatePartitionSnapshot> restoredSnapshots
	) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		long startMills = System.currentTimeMillis();

		stateBackend.createDB();
		for (StatePartitionSnapshot rawSnapshot : restoredSnapshots) {
			Preconditions.checkState(rawSnapshot instanceof DefaultStatePartitionSnapshot);
			DefaultStatePartitionSnapshot snapshot =
				(DefaultStatePartitionSnapshot) rawSnapshot;

			StreamStateHandle snapshotHandle = snapshot.getSnapshotHandle();
			if (snapshotHandle == null) {
				continue;
			}

			FSDataInputStream inputStream = snapshotHandle.openInputStream();
			try {
				DataInputViewStreamWrapper inputView =
					new DataInputViewStreamWrapper(inputStream);

				int numRestoredKeyedStates = inputView.readInt();
				List<KeyedStateDescriptor> keyedStateDescriptors = new ArrayList<>(numRestoredKeyedStates);
				for (int i = 0; i < numRestoredKeyedStates; ++i) {
					KeyedStateDescriptor restoredStateDescriptor =
						InstantiationUtil.deserializeObject(
							inputStream, stateBackend.getUserClassLoader());
					keyedStateDescriptors.add(restoredStateDescriptor);
				}

				int numRestoredSubKeyedStates = inputView.readInt();
				List<SubKeyedStateDescriptor> subKeyedStateDescriptors = new ArrayList<>(numRestoredSubKeyedStates);
				for (int i = 0; i < numRestoredSubKeyedStates; ++i) {
					SubKeyedStateDescriptor restoredStateDescriptor =
						InstantiationUtil.deserializeObject(
							inputStream, stateBackend.getUserClassLoader());

					subKeyedStateDescriptors.add(restoredStateDescriptor);
				}
				int numStates = inputView.readInt();
				for (int i = 0; i < numStates; ++i) {
					String stateName = InstantiationUtil.deserializeObject(
						inputStream, stateBackend.getUserClassLoader());
					Integer id = InstantiationUtil.deserializeObject(
						inputStream, stateBackend.getUserClassLoader());
					id2StateName.put(id, stateName);
				}

				stateBackend.registAllStates(keyedStateDescriptors, subKeyedStateDescriptors);
				Map<Integer, Tuple2<Long, Integer>> metaInfos = snapshot.getMetaInfos();
				restoreData(metaInfos, inputStream, inputView);
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (Exception e) {
						LOG.warn("Could not properly close the input stream.", e);
					}
				}
			}
		}

		long endMills = System.currentTimeMillis();
		LOG.info("Full Restored with RocksDB state backend using {} ms.", endMills - startMills);
	}

	//--------------------------------------------------------------------------

	/**
	 * A helper method to restore the data from the snapshot.
	 *
	 * @param metaInfos The offsets and the number of entries of the groups
	 *                  in the snapshot.
	 * @param inputStream The input stream where the snapshot is read.
	 * @param inputView The input view of the input stream.
	 * @throws IOException Thrown when the backend fails to read the snapshot or
	 *                     to deserialize the state data from the snapshot.
	 */
	private void restoreData(
		Map<Integer, Tuple2<Long, Integer>> metaInfos,
		FSDataInputStream inputStream,
		DataInputView inputView) throws IOException {
		GroupSet groups = stateBackend.getGroups();
		for (int group : groups) {
			Tuple2<Long, Integer> metaInfo = metaInfos.get(group);
			if (metaInfo == null) {
				continue;
			}

			long offset = metaInfo.f0;
			int numEntries = metaInfo.f1;

			inputStream.seek(offset);

			for (int i = 0; i < numEntries; ++i) {
				Integer id = IntSerializer.INSTANCE.deserialize(inputView);
				String cfNameStr = id2StateName.get(id);
				ColumnFamilyHandle columnFamilyHandle = stateBackend.getOrCreateColumnfamily(cfNameStr);
				byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);
				byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(inputView);
				try {
					stateBackend.getDbInstance().put(columnFamilyHandle, key, value);
				} catch (RocksDBException e) {
					throw new StateAccessException(e);
				}
			}
		}
	}
}
