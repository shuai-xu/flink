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

import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The base unit tests to validate that internal states can be correctly saved
 * and restored in the stateBackend.
 */
@RunWith(Parameterized.class)
public abstract class InternalStateCheckpointTestBase extends TestLogger {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	@Parameterized.Parameters(name = "Checkpoint option: {0}")
	public static Collection<CheckpointType> checkpointTypes() {
		return Arrays.asList(CheckpointType.CHECKPOINT, CheckpointType.SAVEPOINT);
	}

	@Parameterized.Parameter
	public CheckpointType checkpointType;

	private CheckpointOptions checkpointOptions;

	protected InternalStateBackend stateBackend;

	protected CheckpointStreamFactory checkpointStreamFactory;

	protected int maxParallelism;

	protected int initParallelism;

	protected int initSubtaskIndex;

	protected ClassLoader classLoader;

	/**
	 * Creates a new state stateBackend for testing.
	 *
	 * @return A new state stateBackend for testing.
	 */
	protected abstract InternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader) throws IOException;

	@Before
	public void open() throws Exception {
		checkpointOptions = checkpointType.equals(CheckpointType.CHECKPOINT) ?
			CheckpointOptions.forCheckpointWithDefaultLocation() :
			new CheckpointOptions(CheckpointType.SAVEPOINT, new CheckpointStorageLocationReference(tmpFolder.newFolder().toURI().toString().getBytes(Charset.defaultCharset())));

		checkpointStreamFactory = new MemCheckpointStreamFactory(4 * 1024 * 1024);

		maxParallelism = 10;
		initParallelism = 1;
		initSubtaskIndex = 0;
		classLoader = ClassLoader.getSystemClassLoader();

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader);
		stateBackend.restore(null);
	}

	@After
	public void close() {
		if (stateBackend != null) {
			stateBackend.close();
		}
	}

	@Test
	public void testCheckpointWithEmptyStateBackend() throws Exception {

		RunnableFuture<StatePartitionSnapshot> snapshotFuture =
			stateBackend.snapshot(0, 0, checkpointStreamFactory, checkpointOptions);
		assertNotNull(snapshotFuture);

		StatePartitionSnapshot snapshot = runSnapshot(snapshotFuture);
		assertNotNull(snapshot);
		stateBackend.close();

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader);
		stateBackend.restore(Collections.singleton(snapshot));

		Collection<InternalState> states = stateBackend.getInternalStates();
		assertTrue(states.isEmpty());
	}

	@Test
	public void testCheckpointWithEmptyState() throws Exception {
		// test empty backend with empty state.
		InternalStateDescriptor globalStateDescriptor =
			new InternalStateDescriptorBuilder("globalState1")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();
		stateBackend.getInternalState(globalStateDescriptor);

		RunnableFuture<StatePartitionSnapshot> snapshotFuture =
			stateBackend.snapshot(0, 0, checkpointStreamFactory, checkpointOptions);
		assertNotNull(snapshotFuture);

		StatePartitionSnapshot snapshot = runSnapshot(snapshotFuture);
		assertNotNull(snapshot);
		stateBackend.close();

		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader);
		stateBackend.restore(Collections.singleton(snapshot));

		Collection<InternalState> states = stateBackend.getInternalStates();
		assertEquals(1, states.size());

		InternalState state = states.iterator().next();
		assertEquals(globalStateDescriptor, state.getDescriptor());
		assertFalse(state.iterator().hasNext());
	}

	@Test
	public void testCheckpointWithoutParallelismChange() throws Exception {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		Map<String, Map<Row, Row>> stateMaps = new HashMap<>();

		InternalStateDescriptor globalStateDescriptor1 =
			new InternalStateDescriptorBuilder("globalState1")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();
		InternalState globalState1 = stateBackend.getInternalState(globalStateDescriptor1);
		populateStateData(stateMaps, globalState1);

		InternalStateDescriptor globalStateDescriptor2 =
			new InternalStateDescriptorBuilder("globalState2")
				.addKeyColumn("key1", IntSerializer.INSTANCE)
				.addKeyColumn("key2", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();
		InternalState globalState2 = stateBackend.getInternalState(globalStateDescriptor2);
		populateStateData(stateMaps, globalState2);

		InternalStateDescriptor emptyStateDescriptor =
			new InternalStateDescriptorBuilder("emptyState")
				.addKeyColumn("key", IntSerializer.INSTANCE)
				.addValueColumn("value", FloatSerializer.INSTANCE)
				.getDescriptor();
		InternalState emptyState = stateBackend.getInternalState(emptyStateDescriptor);

		// Takes a snapshot of the states
		StatePartitionSnapshot snapshot1 =
			runSnapshot(stateBackend, 0, 0, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		// Does some updates to the states
		Random random = new Random();
		int index = 0;
		for (int i = 0; i < 1000; ++i) {
			if (index % 3 == 0) {
				Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
				Row value1 = generateValue(random, globalStateDescriptor1.getNumValueColumns());
				globalState1.put(key1, value1);

				Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
				Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
				globalState2.put(key2, value2);
			}
			index++;
		}

		stateBackend.close();

		// Restores the stateBackend from the snapshot
		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader);
		stateBackend.restore(Collections.singleton(snapshot1));

		// Validates that the states are correctly restored.
		for (Map.Entry<String, Map<Row, Row>> stateEntry : stateMaps.entrySet()) {
			String stateName = stateEntry.getKey();
			InternalState state = stateBackend.getInternalState(stateName);
			assertNotNull(state);

			Map<Row, Row> pairs = stateEntry.getValue();
			Iterator<Pair<Row, Row>> iterator = state.iterator();
			validateStateData(pairs, iterator);
		}

		globalState1 = stateBackend.getInternalState(globalStateDescriptor1);
		assertNotNull(globalState1);

		globalState2 = stateBackend.getInternalState(globalStateDescriptor2);
		assertNotNull(globalState2);

		emptyState = stateBackend.getInternalState(emptyStateDescriptor);
		assertNotNull(emptyState);

		// Does some updates to the states

		index = 0;
		for (int i = 200; i < 1200; ++i) {
			if (index % 4 == 0) {
				Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
				Row value1 = generateValue(random, globalStateDescriptor1.getNumValueColumns());
				globalState1.put(key1, value1);
				recordStatePair(stateMaps, globalState1, key1, value1);

				Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
				Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
				globalState2.put(key2, value2);
				recordStatePair(stateMaps, globalState2, key2, value2);
			}

			index++;
		}

		// Takes a snapshot of the states
		StatePartitionSnapshot snapshot2 =
			runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

		index = 0;
		for (int i = 300; i < 1300; ++i) {
			if (index % 5 == 0) {
				Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
				globalState1.remove(key1);

				Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
				globalState2.remove(key2);
			}

			index++;
		}

		stateBackend.close();

		// Restores the stateBackend from the snapshot
		stateBackend = createStateBackend(
			maxParallelism,
			getGroupsForSubtask(maxParallelism, initParallelism, initSubtaskIndex),
			classLoader);

		stateBackend.restore(Collections.singleton(snapshot2));

		// Validates that the states are correctly restored.
		for (Map.Entry<String, Map<Row, Row>> stateEntry : stateMaps.entrySet()) {
			String stateName = stateEntry.getKey();
			InternalState state = stateBackend.getInternalState(stateName);
			assertNotNull(state);

			Map<Row, Row> pairs = stateEntry.getValue();
			Iterator<Pair<Row, Row>> iterator = state.iterator();

			validateStateData(pairs, iterator);
		}

	}

	/**
	 * Test checkpoint and restore when parallelism changed, from 1 -> 3 -> 2.
	 */
	@Test
	public void testCheckpointWithParallelismChange() throws Exception {
		if (checkpointType.equals(CheckpointType.SAVEPOINT)) {
			SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

			Map<Integer, Map<String, Map<Row, Row>>> globalGroupMaps = new HashMap<>();

			InternalStateDescriptor globalStateDescriptor1 =
				new InternalStateDescriptorBuilder("globalState1")
					.addKeyColumn("key", IntSerializer.INSTANCE)
					.addValueColumn("value", FloatSerializer.INSTANCE)
					.getDescriptor();
			InternalState globalState1 = stateBackend.getInternalState(globalStateDescriptor1);
			populateGroupStateData(globalGroupMaps, globalState1);

			InternalStateDescriptor globalStateDescriptor2 =
				new InternalStateDescriptorBuilder("globalState2")
					.addKeyColumn("key1", IntSerializer.INSTANCE)
					.addKeyColumn("key2", IntSerializer.INSTANCE)
					.addValueColumn("value", FloatSerializer.INSTANCE)
					.getDescriptor();
			InternalState globalState2 = stateBackend.getInternalState(globalStateDescriptor2);
			populateGroupStateData(globalGroupMaps, globalState2);

			InternalStateDescriptor emptyStateDescriptor =
				new InternalStateDescriptorBuilder("emptyState")
					.addKeyColumn("key", IntSerializer.INSTANCE)
					.addValueColumn("value", FloatSerializer.INSTANCE)
					.getDescriptor();
			InternalState emptyState = stateBackend.getInternalState(emptyStateDescriptor);

			// Takes a snapshot of the states
			StatePartitionSnapshot snapshot1 =
				runSnapshot(stateBackend, 0, 0, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

			// Does some updates to the states
			Random random = new Random();
			int index = 0;
			for (int i = 0; i < 1000; ++i) {
				if (index % 3 == 0) {
					Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
					Row value1 = generateValue(random, globalStateDescriptor1.getNumValueColumns());
					globalState1.put(key1, value1);

					Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
					Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
					globalState2.put(key2, value2);
				}
				index++;
			}

			stateBackend.close();

			// Restores the stateBackend from a subset of the snapshot
			GroupRange firstGroups1 = (GroupRange) getGroupsForSubtask(maxParallelism, 3, 0);
			StatePartitionSnapshot firstSnapshot1 = snapshot1.getIntersection(firstGroups1);

			// Restores the stateBackend from the snapshot
			stateBackend = createStateBackend(maxParallelism, firstGroups1, classLoader);
			stateBackend.restore(Collections.singleton(firstSnapshot1));

			// Validates that the states are correctly restored.
			globalState1 = stateBackend.getInternalState(globalStateDescriptor1.getName());
			assertNotNull(globalState1);

			globalState2 = stateBackend.getInternalState(globalStateDescriptor2.getName());
			assertNotNull(globalState2);

			emptyState = stateBackend.getInternalState(emptyStateDescriptor.getName());
			assertNotNull(emptyState);

			Iterator<Pair<Row, Row>> globalIterator1 = globalState1.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator1, firstGroups1, globalStateDescriptor1.getName());

			Iterator<Pair<Row, Row>> globalIterator2 = globalState2.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator2, firstGroups1, globalStateDescriptor2.getName());

			// Does some updates to the stateBackend.
			index = 0;
			for (int i = 200; i < 1200; ++i) {
				if (index % 2 == 0) {
					Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
					if (isGroupContainsKey(firstGroups1, key1)) {
						Row value1 = generateValue(random, globalStateDescriptor1.getNumValueColumns());
						globalState1.put(key1, value1);
						recordGroupPair(globalGroupMaps, globalState1, key1, value1);
					}

					Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
					if (isGroupContainsKey(firstGroups1, key2)) {
						Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
						globalState2.put(key2, value2);
						recordGroupPair(globalGroupMaps, globalState2, key2, value2);
					}
				}

				index++;
			}

			// Takes a snapshot of the stateBackend
			StatePartitionSnapshot firstSnapshot2 =
				runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

			stateBackend.close();

			// Restores the stateBackend from a subset of the snapshot
			GroupRange secondGroups1 = (GroupRange) getGroupsForSubtask(maxParallelism, 3, 1);
			StatePartitionSnapshot secondSnapshot1 = snapshot1.getIntersection(secondGroups1);

			// Restores the stateBackend from the snapshot
			stateBackend = createStateBackend(maxParallelism, secondGroups1, classLoader);
			stateBackend.restore(Collections.singleton(secondSnapshot1));

			// Validates that the states are correctly restored.
			globalState1 = stateBackend.getInternalState(globalStateDescriptor1.getName());
			assertNotNull(globalState1);

			globalState2 = stateBackend.getInternalState(globalStateDescriptor2.getName());
			assertNotNull(globalState2);

			emptyState = stateBackend.getInternalState(emptyStateDescriptor.getName());
			assertNotNull(emptyState);

			globalIterator1 = globalState1.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator1, secondGroups1, globalStateDescriptor1.getName());

			globalIterator2 = globalState2.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator2, secondGroups1, globalStateDescriptor2.getName());

			// Does some updates to the stateBackend.
			index = 0;
			for (int i = 200; i < 1200; ++i) {
				if (index % 3 == 0) {
					Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
					if (isGroupContainsKey(secondGroups1, key1)) {
						Row value1 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
						globalState1.put(key1, value1);
						recordGroupPair(globalGroupMaps, globalState1, key1, value1);
					}

					Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
					if (isGroupContainsKey(secondGroups1, key2)) {
						Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
						globalState2.put(key2, value2);

						recordGroupPair(globalGroupMaps, globalState2, key2, value2);
					}
				}

				index++;
			}

			// Takes a snapshot of the stateBackend
			StatePartitionSnapshot secondSnapshot2 =
				runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

			stateBackend.close();

			// Restores the stateBackend from a subset of the snapshot
			GroupRange thirdGroups1 = (GroupRange) getGroupsForSubtask(maxParallelism, 3, 2);
			StatePartitionSnapshot thirdSnapshot1 = snapshot1.getIntersection(thirdGroups1);

			// Restores the stateBackend from the snapshot
			stateBackend = createStateBackend(maxParallelism, thirdGroups1, classLoader);
			stateBackend.restore(Collections.singleton(thirdSnapshot1));

			// Validates that the states are correctly restored.
			globalState1 = stateBackend.getInternalState(globalStateDescriptor1.getName());
			assertNotNull(globalState1);

			globalState2 = stateBackend.getInternalState(globalStateDescriptor2.getName());
			assertNotNull(globalState2);

			emptyState = stateBackend.getInternalState(emptyStateDescriptor.getName());
			assertNotNull(emptyState);

			globalIterator1 = globalState1.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator1, thirdGroups1, globalStateDescriptor1.getName());

			globalIterator2 = globalState2.iterator();
			validateStateDataWithGroupSet(globalGroupMaps, globalIterator2, thirdGroups1, globalStateDescriptor2.getName());


			// Does some updates to the stateBackend.
			index = 0;
			for (int i = 200; i < 1200; ++i) {
				if (index % 4 == 0) {
					Row key1 = generateKey(i, globalStateDescriptor1.getNumKeyColumns());
					if (isGroupContainsKey(thirdGroups1, key1)) {
						Row value1 = generateValue(random, globalStateDescriptor1.getNumValueColumns());
						globalState1.put(key1, value1);
						recordGroupPair(globalGroupMaps, globalState1, key1, value1);
					}

					Row key2 = generateKey(i, globalStateDescriptor2.getNumKeyColumns());
					if (isGroupContainsKey(thirdGroups1, key2)) {
						Row value2 = generateValue(random, globalStateDescriptor2.getNumValueColumns());
						globalState2.put(key2, value2);
						recordGroupPair(globalGroupMaps, globalState2, key2, value2);
					}
				}

				index++;
			}

			// Takes a snapshot of the stateBackend
			StatePartitionSnapshot thirdSnapshot2 =
				runSnapshot(stateBackend, 1, 1, checkpointStreamFactory, checkpointOptions, sharedStateRegistry);

			stateBackend.close();

			// Merge the local states
			GroupSet leftGroups3 = getGroupsForSubtask(maxParallelism, 2, 0);
			GroupSet rightGroups3 = getGroupsForSubtask(maxParallelism, 2, 1);
			InternalStateBackend newLeftBackend = createStateBackend(maxParallelism, leftGroups3, classLoader);
			InternalStateBackend newRightBackend = createStateBackend(maxParallelism, rightGroups3, classLoader);

			try {
				StatePartitionSnapshot firstSnapshot3 = firstSnapshot2.getIntersection(leftGroups3);
				StatePartitionSnapshot secondSnapshot3ForLeft = secondSnapshot2.getIntersection(leftGroups3);
				StatePartitionSnapshot secondSnapshot3ForRight = secondSnapshot2.getIntersection(rightGroups3);
				StatePartitionSnapshot thirdSnapshot3 = thirdSnapshot2.getIntersection(rightGroups3);

				newLeftBackend.restore(Arrays.asList(firstSnapshot3, secondSnapshot3ForLeft));
				newRightBackend.restore(Arrays.asList(secondSnapshot3ForRight, thirdSnapshot3));

				// Validates that the states are correctly restored.
				InternalState leftGlobalState1 = newLeftBackend.getInternalState(globalStateDescriptor1.getName());
				assertNotNull(leftGlobalState1);

				InternalState rightGlobalState1 = newRightBackend.getInternalState(globalStateDescriptor1.getName());
				assertNotNull(rightGlobalState1);

				InternalState leftGlobalState2 = newLeftBackend.getInternalState(globalStateDescriptor2.getName());
				assertNotNull(leftGlobalState2);

				InternalState rightGlobalState2 = newRightBackend.getInternalState(globalStateDescriptor2.getName());
				assertNotNull(rightGlobalState2);

				emptyState = newLeftBackend.getInternalState(emptyStateDescriptor.getName());
				assertNotNull(emptyState);

				emptyState = newRightBackend.getInternalState(emptyStateDescriptor.getName());
				assertNotNull(emptyState);

				Iterator<Pair<Row, Row>> leftGlobalIterator1 = leftGlobalState1.iterator();
				validateStateDataWithGroupSet(globalGroupMaps, leftGlobalIterator1, leftGroups3, globalStateDescriptor1.getName());

				Iterator<Pair<Row, Row>> rightGlobalIterator1 = rightGlobalState1.iterator();
				validateStateDataWithGroupSet(globalGroupMaps, rightGlobalIterator1, rightGroups3, globalStateDescriptor1.getName());

				Iterator<Pair<Row, Row>> leftGlobalIterator2 = leftGlobalState2.iterator();
				validateStateDataWithGroupSet(globalGroupMaps, leftGlobalIterator2, leftGroups3, globalStateDescriptor2.getName());

				Iterator<Pair<Row, Row>> rightGlobalIterator2 = rightGlobalState2.iterator();
				validateStateDataWithGroupSet(globalGroupMaps, rightGlobalIterator2, rightGroups3, globalStateDescriptor2.getName());

			} finally {
				newLeftBackend.close();
				newRightBackend.close();
			}
		}
	}

	//--------------------------------------------------------------------------

	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}

	private boolean isGroupContainsKey(GroupSet groups, Row key) {
		return groups.contains(HashPartitioner.INSTANCE.partition(key, maxParallelism));
	}

	private static Row generateKey(int number, int numKeyColumns) {
		Row key = new Row(numKeyColumns);
		for (int j = 0; j < numKeyColumns; ++j) {
			if (j == numKeyColumns - 1) {
				key.setField(j, number);
			} else {
				key.setField(j, number % 10);
			}
			number /= 10;
		}
		return key;
	}

	private static Row generateValue(Random random, int numValueColumns) {
		Row value = new Row(numValueColumns);
		for (int j = 0; j < numValueColumns; ++j) {
			value.setField(j, random.nextFloat());
		}
		return value;
	}

	private static void recordGroupPair(Map<Integer, Map<String, Map<Row, Row>>> groupMaps, InternalState state, Row key, Row value) {
		InternalStateDescriptor stateDescriptor = state.getDescriptor();
		String stateName = stateDescriptor.getName();
		Partitioner<Row> partitioner = stateDescriptor.getPartitioner();

		int group = partitioner.partition(key, state.getNumGroups());
		Map<String, Map<Row, Row>> stateMaps = groupMaps.computeIfAbsent(group, k -> new HashMap<>());
		Map<Row, Row> pairs = stateMaps.computeIfAbsent(stateName, k -> new HashMap<>());
		pairs.put(key, value);
	}

	private static void populateGroupStateData(Map<Integer, Map<String, Map<Row, Row>>> groupMaps, InternalState state) {
		Random random = new Random();

		InternalStateDescriptor stateDescriptor = state.getDescriptor();
		int numKeyColumns = stateDescriptor.getNumKeyColumns();
		int numValueColumns = stateDescriptor.getNumValueColumns();

		for (int i = 0; i < 1000; ++i) {
			Row key = generateKey(i, numKeyColumns);
			Row value = generateValue(random, numValueColumns);

			state.put(key, value);
			recordGroupPair(groupMaps, state, key, value);
		}
	}

	private static void populateStateData(Map<String, Map<Row, Row>> stateMaps, InternalState state) {
		Random random = new Random();

		InternalStateDescriptor stateDescriptor = state.getDescriptor();
		int numKeyColumns = stateDescriptor.getNumKeyColumns();
		int numValueColumns = stateDescriptor.getNumValueColumns();

		for (int i = 0; i < 1000; ++i) {
			Row key = generateKey(i, numKeyColumns);
			Row value = generateValue(random, numValueColumns);

			state.put(key, value);
			recordStatePair(stateMaps, state, key, value);
		}
	}

	private static void recordStatePair(Map<String, Map<Row, Row>> stateMaps, InternalState state, Row key, Row value) {
		InternalStateDescriptor stateDescriptor = state.getDescriptor();

		String stateName = stateDescriptor.getName();
		Map<Row, Row> stateMap = stateMaps.computeIfAbsent(stateName, k -> new HashMap<>());

		stateMap.put(key, value);
	}

	private static void validateStateData(Map<Row, Row> pairs, Iterator<Pair<Row, Row>> iterator) {
		assertNotNull(iterator);

		if (pairs == null || pairs.isEmpty()) {
			assertFalse(iterator.hasNext());
			return;
		}

		int numActualPairs = 0;
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();

			Row actualValue = pair.getValue();
			Row expectedValue = pairs.get(pair.getKey());
			assertEquals(expectedValue, actualValue);

			numActualPairs++;
		}

		assertEquals(pairs.size(), numActualPairs);
	}

	private static void validateStateDataWithGroupSet(
		Map<Integer, Map<String, Map<Row, Row>>> groupMaps,
		Iterator<Pair<Row, Row>> iterator,
		GroupSet groups, String stateName) {

		assertNotNull(iterator);

		if (groupMaps == null) {
			assertFalse(iterator.hasNext());
			return;
		}

		Map<Row, Row> pairs = new HashMap<>();
		for (Integer group : groups) {
			Map<String, Map<Row, Row>> stateMap = groupMaps.get(group);
			if (stateMap != null) {
				Map<Row, Row> rowMap = stateMap.get(stateName);
				if (rowMap != null) {
					pairs.putAll(rowMap);
				}
			}
		}

		if (pairs.isEmpty()) {
			assertFalse(iterator.hasNext());
			return;
		}

		int numActualPairs = 0;
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();

			Row actualValue = pair.getValue();
			Row expectedValue = pairs.get(pair.getKey());
			assertEquals(expectedValue, actualValue);

			numActualPairs++;
		}

		assertEquals(pairs.size(), numActualPairs);
	}

	private static StatePartitionSnapshot runSnapshot(
		InternalStateBackend stateBackend,
		long checkpointId,
		long checkpointTimestamp,
		CheckpointStreamFactory checkpointStreamFactory,
		CheckpointOptions checkpointOptions,
		SharedStateRegistry sharedStateRegistry
	) throws Exception {

		RunnableFuture<StatePartitionSnapshot> snapshotFuture =
			stateBackend.snapshot(checkpointId, checkpointTimestamp, checkpointStreamFactory, checkpointOptions);

		StatePartitionSnapshot statePartitionSnapshot = FutureUtil.runIfNotDoneAndGet(snapshotFuture);

		// Register the snapshot at the registry to replace the place holders with actual handles.
		if (checkpointOptions.getCheckpointType().equals(CheckpointType.CHECKPOINT)) {
			statePartitionSnapshot.registerSharedStates(sharedStateRegistry);
		}

		return statePartitionSnapshot;
	}

	private StatePartitionSnapshot runSnapshot(RunnableFuture<StatePartitionSnapshot> snapshotRunnableFuture) throws Exception {
		if (!snapshotRunnableFuture.isDone()) {
			Thread runner = new Thread(snapshotRunnableFuture);
			runner.start();
		}
		return snapshotRunnableFuture.get();
	}
}
