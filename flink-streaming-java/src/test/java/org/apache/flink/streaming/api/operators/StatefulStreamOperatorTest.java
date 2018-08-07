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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupRangePartitioner;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateBackend;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueStateDescriptor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests to verify that stream operators can correctly access, save and
 * restore the states.
 */
public class StatefulStreamOperatorTest {

	/**
	 * Verify that the operator can correctly create the configured state
	 * backend.
	 */
	@Test
	public void testStateBackendCreation() throws Exception {
		Configuration configuration = new Configuration();
		//TODO: How to setup the stateBackend type???
//		configuration.setString(CoreOptions.STATE_BACKEND_CLASSNAME, TestInternalStateBackend.class.getName());

		MockEnvironment mockEnvironment = new MockEnvironment(
			new JobID(),
			new JobVertexID(new byte[16]),
			"Test Task",
			32L * 1024L,
			new MockInputSplitProvider(),
			1,
			new Configuration(),
			new ExecutionConfig(),
			new TestTaskStateManager(),
			1,
			1,
			0,
			Thread.currentThread().getContextClassLoader());

		InternalStateAccessOperator operator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator, mockEnvironment);

//		testHarness.setConfiguration(configuration);
		testHarness.setup();

		AbstractInternalStateBackend stateBackend = Whitebox.getInternalState(operator, "internalStateBackend");
		assertTrue(stateBackend instanceof AbstractInternalStateBackend);
	}

	/**
	 * Verify that the operator can correctly access keyed states.
	 */
	@Test
	public void testKeyedStateAccess() throws Exception {
		KeyedStateAccessOperator operator = new KeyedStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:HELLO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K2:WORLD")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:CIAO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("REMOVE", "K2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));

		List<String> expectedResults = Arrays.asList("HELLO", "WORLD", "CIAO");
		List<String> actualResults = testHarness.extractOutputValues();
		assertEquals(expectedResults, actualResults);

		testHarness.close();
	}

	/**
	 * Verify that the operator can correctly access subkeyed states.
	 */
	@Test
	public void testSubKeyedStateAccess() throws Exception {
		SubKeyedStateAccessOperator operator = new SubKeyedStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:N1:TOM")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K2:N1:JIM")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:N2:LUCY")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1:N1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1:N2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K2:N1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K2:N1:LILY")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("REMOVE", "K1:N2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1:N1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K2:N1")));

		List<String> expectedResults = Arrays.asList("TOM", "LUCY", "JIM", "TOM", "LILY");
		List<String> actualResults = testHarness.extractOutputValues();
		assertEquals(expectedResults, actualResults);

		testHarness.close();
	}

	/**
	 * Verify that the operator can correctly access internal states.
	 */
	@Test
	public void testInternalStateAccess() throws Exception {
		InternalStateAccessOperator operator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:HELLO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K2:WORLD")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:CIAO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("REMOVE", "K2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));

		List<String> expectedResults = Arrays.asList("HELLO", "WORLD", "CIAO");
		List<String> actualResults = testHarness.extractOutputValues();
		assertEquals(expectedResults, actualResults);

		testHarness.close();
	}

	/**
	 * Verify that the operator can correctly save and restore states in the
	 * cases the degree of parallelism is not changed.
	 */
	@Test
	public void testStateCheckpointWithoutParallelismChange() throws Exception {
		InternalStateAccessOperator operator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:HELLO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K2:WORLD")));

		OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:CIAO")));

		testHarness.close();

		operator = new InternalStateAccessOperator();
		testHarness = new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K2")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", "K1:CIAO")));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", "K1")));

		List<String> expectedResults = Arrays.asList("HELLO", "WORLD", "CIAO");
		List<String> actualResults = testHarness.extractOutputValues();
		assertEquals(expectedResults, actualResults);

		testHarness.close();
	}

	/**
	 * Verify that the operator can correctly save and restore states in the
	 * cases the degree of parallelism is changed.
	 */
	@Test
	public void testStateCheckpointWithParallelismChange() throws Exception {
		InternalStateAccessOperator operator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator, 10, 1, 0);

		testHarness.open();

		GroupRange leftGroups = GroupRangePartitioner.getPartitionRange(GroupRange.of(0, 10), 3, 0);
		Map<String, String> leftPairs = new HashMap<>();

		GroupRange rightGroups = GroupRangePartitioner.getPartitionRange(GroupRange.of(0, 10), 3, 2);
		Map<String, String> rightPairs = new HashMap<>();

		Random random = new Random();
		for (int i = 0; i < 100; ++i) {
			String key = "K" + Integer.toString(i);
			String value = "V" + Integer.toString(random.nextInt(1000));

			int group = HashPartitioner.INSTANCE.partition(Row.of(key), 10);
			if (leftGroups.contains(group)) {
				leftPairs.put(key, value);
			} else if (rightGroups.contains(group)) {
				rightPairs.put(key, value);
			}

			testHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", key + ":" + value)));
		}

		OperatorSubtaskState operatorSnapshot = testHarness.snapshot(0L, 0L);

		testHarness.close();

		// Verify that the operator instances can correctly restore their states
		// when the degree of parallelism increases.

		OperatorSubtaskState leftStateSnapshot = getSplitSnapshot(operatorSnapshot, leftGroups);
		InternalStateAccessOperator leftOperator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> leftTestHarness =
			new OneInputStreamOperatorTestHarness<>(leftOperator, 10, 3, 0);
		leftTestHarness.setup();
		leftTestHarness.initializeState(leftStateSnapshot);
		leftTestHarness.open();

		List<String> leftExpectedResults = new ArrayList<>();
		for (Map.Entry<String, String> leftPair : leftPairs.entrySet()) {
			leftTestHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", leftPair.getKey())));
			leftExpectedResults.add(leftPair.getValue());
		}
		List<String> leftActualResults = leftTestHarness.extractOutputValues();
		assertEquals(leftExpectedResults, leftActualResults);

		OperatorSubtaskState rightStateSnapshot = getSplitSnapshot(operatorSnapshot, rightGroups);
		InternalStateAccessOperator rightOperator = new InternalStateAccessOperator();
		OneInputStreamOperatorTestHarness<Tuple2<String, String>, String> rightTestHarness =
			new OneInputStreamOperatorTestHarness<>(rightOperator, 10, 3, 2);
		rightTestHarness.setup();
		rightTestHarness.initializeState(rightStateSnapshot);
		rightTestHarness.open();

		List<String> rightExpectedResults = new ArrayList<>();
		for (Map.Entry<String, String> rightPair : rightPairs.entrySet()) {
			rightTestHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", rightPair.getKey())));
			rightExpectedResults.add(rightPair.getValue());
		}
		List<String> rightActualResults = rightTestHarness.extractOutputValues();
		assertEquals(rightExpectedResults, rightActualResults);

		// Do some updates to the states in the operator instances.

		Map<String, String> pairs = new HashMap<>();
		pairs.putAll(leftPairs);
		pairs.putAll(rightPairs);

		for (int i = 20; i < 120; ++i) {
			String key = "K" + Integer.toString(i);
			String value = "V" + Integer.toString(random.nextInt(1000));

			int group = HashPartitioner.INSTANCE.partition(Row.of(key), 10);
			if (leftGroups.contains(group)) {
				leftTestHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", key + ":" + value)));
				pairs.put(key, value);
			} else if (rightGroups.contains(group)) {
				rightTestHarness.processElement(new StreamRecord<>(new Tuple2<>("ADD", key + ":" + value)));
				pairs.put(key, value);
			}
		}

		// Verify that the operator can correctly restore the state when the
		// degree of parallelism decreases.

		OperatorSubtaskState leftOperatorSnapshot1 = leftTestHarness.snapshot(1, 1);
		leftTestHarness.close();

		OperatorSubtaskState rightOperatorSnapshot1 = rightTestHarness.snapshot(1, 1);
		rightTestHarness.close();

		operator = new InternalStateAccessOperator();
		testHarness = new OneInputStreamOperatorTestHarness<>(operator, 10, 1, 0);
		testHarness.setup();
		testHarness.initializeState(mergeSnapshot(leftOperatorSnapshot1, rightOperatorSnapshot1));
		testHarness.open();

		List<String> expectedResults = new ArrayList<>();
		for (Map.Entry<String, String> pair : pairs.entrySet()) {
			expectedResults.add(pair.getValue());
			testHarness.processElement(new StreamRecord<>(new Tuple2<>("GET", pair.getKey())));
		}
		List<String> actualResults = testHarness.extractOutputValues();
		assertEquals(expectedResults, actualResults);

		testHarness.close();
	}

	//--------------------------------------------------------------------------

	/**
	 * An implementation of {@link InternalStateBackend} for testing.
	 */
	public static class TestInternalStateBackend extends AbstractInternalStateBackend {

		public TestInternalStateBackend() {
			super(10, GroupRange.of(0, 10), Thread.currentThread().getContextClassLoader());
		}

		private static final long serialVersionUID = 1661814222618778988L;

		@Override
		protected InternalState createInternalState(InternalStateDescriptor stateDescriptor) {
			return null;
		}

		@Override
		public void close() {

		}

		@Override
		public InternalState getInternalState(InternalStateDescriptor stateDescriptor) {
			return null;
		}

		@Override
		public InternalState getInternalState(String stateName) {
			return null;
		}

		@Override
		public <K, V, S extends KeyedState<K, V>> S getKeyedState(
			KeyedStateDescriptor<K, V, S> stateDescriptor
		) {
			return null;
		}

		@Override
		public <K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
			SubKeyedStateDescriptor<K, N, V, S> stateDescriptor
		) {
			return null;
		}

		@Override
		protected void closeImpl() {

		}

		@Override
		public RunnableFuture<StatePartitionSnapshot> snapshot(long checkpointId, long timestamp, CheckpointStreamFactory streamFactory, CheckpointOptions checkpointOptions) throws Exception {
			return null;
		}

		@Override
		public void restore(Collection<StatePartitionSnapshot> state) throws Exception {

		}
	}

	//--------------------------------------------------------------------------

	private static class InternalStateAccessOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<Tuple2<String, String>, String> {

		private static final long serialVersionUID = 3569321590826278498L;

		private transient InternalState state;

		@Override
		public void open() {
			InternalStateDescriptor stateDescriptor =
				new InternalStateDescriptorBuilder("test-state")
					.addKeyColumn("key", StringSerializer.INSTANCE)
					.addValueColumn("value", StringSerializer.INSTANCE)
					.getDescriptor();

			state = getInternalState(stateDescriptor);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<String, String>> element) throws Exception {
			String command = element.getValue().f0;
			String[] args = element.getValue().f1.split(":");

			switch(command) {
				case "ADD":
					state.put(Row.of(args[0]), Row.of(args[1]));
					break;
				case "REMOVE":
					state.remove(Row.of(args[0]));
					break;
				case "GET":
					String value = (String) state.get(Row.of(args[0])).getField(0);
					output.collect(new StreamRecord<>(value, 0));
					break;
				default:
					throw new IllegalStateException();
			}
		}
	}

	private static class KeyedStateAccessOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<Tuple2<String, String>, String> {

		private static final long serialVersionUID = 1469873169076365349L;

		private transient KeyedValueState<String, String> state;

		@Override
		public void open() {
			KeyedValueStateDescriptor<String, String> stateDescriptor =
				new KeyedValueStateDescriptor<>(
					"test-state",
					StringSerializer.INSTANCE,
					StringSerializer.INSTANCE
				);
			state = getKeyedState(stateDescriptor);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<String, String>> element) throws Exception {
			String command = element.getValue().f0;
			String[] args = element.getValue().f1.split(":");

			switch (command) {
				case "ADD":
					state.put(args[0], args[1]);
					break;
				case "REMOVE":
					state.remove(args[0]);
					break;
				case "GET":
					String value = state.get(args[0]);
					output.collect(new StreamRecord<>(value, 0));
					break;
				default:
					throw new IllegalStateException();
			}
		}
	}

	private static class SubKeyedStateAccessOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<Tuple2<String, String>, String> {

		private static final long serialVersionUID = 2078008089033123739L;

		private transient SubKeyedValueState<String, String, String> state;

		@Override
		public void open() {
			SubKeyedValueStateDescriptor<String, String, String> stateDescriptor =
				new SubKeyedValueStateDescriptor<>(
					"test-state",
					StringSerializer.INSTANCE,
					StringSerializer.INSTANCE,
					StringSerializer.INSTANCE
				);
			state = getSubKeyedState(stateDescriptor);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<String, String>> element) throws Exception {
			String command = element.getValue().f0;
			String[] args = element.getValue().f1.split(":");

			switch (command) {
				case "ADD":
					state.put(args[0], args[1], args[2]);
					break;
				case "REMOVE":
					state.remove(args[0], args[1]);
					break;
				case "GET":
					String value = state.get(args[0], args[1]);
					output.collect(new StreamRecord<>(value, 0));
					break;
				default:
					throw new IllegalStateException();
			}
		}
	}

	private OperatorSubtaskState getSplitSnapshot(OperatorSubtaskState original, GroupRange range) {

		// managed keyed state
		StateObjectCollection<KeyedStateHandle> managedKeyedState = original.getManagedKeyedState();
		List<KeyedStateHandle> newManagedKeyedList = new ArrayList<>();
		for (KeyedStateHandle stateHandle : managedKeyedState) {
			newManagedKeyedList.add(stateHandle.getIntersection(new KeyGroupRange(range.getStartGroup(), range.getEndGroup() - 1)));
		}
		StateObjectCollection<KeyedStateHandle> newManagedKeyedState = new StateObjectCollection<>();
		newManagedKeyedList.addAll(newManagedKeyedList);

		// raw keyed state
		StateObjectCollection<KeyedStateHandle> rawKeyedState = original.getRawKeyedState();
		List<KeyedStateHandle> newRawKeyedList = new ArrayList<>();
		for (KeyedStateHandle stateHandle : rawKeyedState) {
			newRawKeyedList.add(stateHandle.getIntersection(new KeyGroupRange(range.getStartGroup(), range.getEndGroup() - 1)));
		}
		StateObjectCollection<KeyedStateHandle> newRawKeyedState = new StateObjectCollection<>();
		newRawKeyedState.addAll(newRawKeyedList);

		// managed internal state
		StateObjectCollection<StatePartitionSnapshot> managedInternalState = original.getManagedInternalState();
		List<StatePartitionSnapshot> newInternalList = new ArrayList<>();
		for (StatePartitionSnapshot snapshot : managedInternalState) {
			newInternalList.add(snapshot.getIntersection(range));
		}
		StateObjectCollection<StatePartitionSnapshot> newManagedInternalState = new StateObjectCollection<>();
		newManagedInternalState.addAll(newInternalList);

		// managed operator state
		StateObjectCollection<OperatorStateHandle> managedOperatorState = original.getManagedOperatorState();
		List<OperatorStateHandle> newManagedOperatorList = new ArrayList<>();
		int idx = 0;
		for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
			if (idx >= range.getStartGroup() && idx < range.getEndGroup()) {
				newManagedOperatorList.add(operatorStateHandle);
			}
		}
		StateObjectCollection<OperatorStateHandle> newManagedOperatorState = new StateObjectCollection<>();
		newManagedOperatorState.addAll(newManagedOperatorList);

		// raw operator state
		StateObjectCollection<OperatorStateHandle> rawOperatorState = original.getRawOperatorState();
		List<OperatorStateHandle> newRawOperatorList = new ArrayList<>();
		idx = 0;
		for (OperatorStateHandle operatorStateHandle : rawOperatorState) {
			if (idx >= range.getStartGroup() && idx < range.getEndGroup()) {
				newManagedOperatorList.add(operatorStateHandle);
			}
		}
		StateObjectCollection<OperatorStateHandle> newRawOperatorState = new StateObjectCollection<>();
		newRawOperatorState.addAll(newRawOperatorList);

		return new OperatorSubtaskState(
			newManagedOperatorState,
			newRawOperatorState,
			newManagedKeyedState,
			newRawKeyedState,
			newManagedInternalState);
	}

	private OperatorSubtaskState mergeSnapshot(OperatorSubtaskState left, OperatorSubtaskState right) {
		// managed keyed state
		StateObjectCollection<KeyedStateHandle> leftmanagedKeyedState = left.getManagedKeyedState();
		StateObjectCollection<KeyedStateHandle> rightmanagedKeyedState = right.getManagedKeyedState();
		List<KeyedStateHandle> newManagedKeyedList = new ArrayList<>();
		for (KeyedStateHandle stateHandle : leftmanagedKeyedState) {
			newManagedKeyedList.add(stateHandle);
		}
		for (KeyedStateHandle stateHandle : rightmanagedKeyedState) {
			newManagedKeyedList.add(stateHandle);
		}
		StateObjectCollection<KeyedStateHandle> newManagedKeyedState = new StateObjectCollection<>();
		newManagedKeyedList.addAll(newManagedKeyedList);

		// raw keyed state
		StateObjectCollection<KeyedStateHandle> leftrawKeyedState = left.getRawKeyedState();
		StateObjectCollection<KeyedStateHandle> rightrawKeyedState = right.getRawKeyedState();
		List<KeyedStateHandle> newRawKeyedList = new ArrayList<>();
		for (KeyedStateHandle stateHandle : leftrawKeyedState) {
			newRawKeyedList.add(stateHandle);
		}
		for (KeyedStateHandle stateHandle : rightrawKeyedState) {
			newRawKeyedList.add(stateHandle);
		}
		StateObjectCollection<KeyedStateHandle> newRawKeyedState = new StateObjectCollection<>();
		newRawKeyedState.addAll(newRawKeyedList);

		// managed internal state
		StateObjectCollection<StatePartitionSnapshot> leftmanagedInternalState = left.getManagedInternalState();
		StateObjectCollection<StatePartitionSnapshot> rightmanagedInternalState = right.getManagedInternalState();
		List<StatePartitionSnapshot> newInternalList = new ArrayList<>();
		for (StatePartitionSnapshot snapshot : leftmanagedInternalState) {
			newInternalList.add(snapshot);
		}
		for (StatePartitionSnapshot snapshot : rightmanagedInternalState) {
			newInternalList.add(snapshot);
		}
		StateObjectCollection<StatePartitionSnapshot> newManagedInternalState = new StateObjectCollection<>();
		newManagedInternalState.addAll(newInternalList);

		// managed operator state
		List<OperatorStateHandle> managedOperatorList = new ArrayList<>();
		for (OperatorStateHandle operatorStateHandle : left.getManagedOperatorState()) {
			managedOperatorList.add(operatorStateHandle);
		}
		for (OperatorStateHandle operatorStateHandle : right.getManagedOperatorState()) {
			managedOperatorList.add(operatorStateHandle);
		}
		StateObjectCollection<OperatorStateHandle> newManagedOperatorState = new StateObjectCollection<>();
		newManagedOperatorState.addAll(managedOperatorList);

		// raw operator state
		List<OperatorStateHandle> rawOperatorList = new ArrayList<>();
		for (OperatorStateHandle operatorStateHandle : left.getRawOperatorState()) {
			rawOperatorList.add(operatorStateHandle);
		}
		for (OperatorStateHandle operatorStateHandle : right.getRawOperatorState()) {
			rawOperatorList.add(operatorStateHandle);
		}
		StateObjectCollection<OperatorStateHandle> newRawOperatorState = new StateObjectCollection<>();
		newRawOperatorState.addAll(rawOperatorList);

		return new OperatorSubtaskState(
			newManagedOperatorState,
			newRawOperatorState,
			newManagedKeyedState,
			newRawKeyedState,
			newManagedInternalState);
	}
}
