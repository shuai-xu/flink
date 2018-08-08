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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupRangePartitioner;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SubKeyedValueState}.
 */
public class SubKeyedValueStateTest {

	protected AbstractInternalStateBackend backend;

	@Before
	public void openStateBackend() throws Exception {
		backend = new HeapInternalStateBackend(
			10,
			getGroupsForSubtask(10, 1, 0),
			ClassLoader.getSystemClassLoader(),
			TestLocalRecoveryConfig.disabled()
		);

		backend.restore(null);
	}

	@After
	public void closeStateBackend() {
		if (backend != null) {
			backend.close();
		}
	}

	@Test
	public void testKeyAndNamespaceAccess() {

		SubKeyedValueStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedValueStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		SubKeyedValueState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Map<String, Float>> keyMap = new HashMap<>();
		keyMap.put(0, Collections.singletonMap("0", null));
		for (int i = 1; i < 10; ++i) {
			Map<String, Float> namespaceMap = new HashMap<>();
			for (int j = 0; j < i; ++j) {
				String namespace = Integer.toString(random.nextInt(10000));
				namespaceMap.put(namespace, random.nextFloat());
			}

			keyMap.put(i, namespaceMap);
		}

		// Validates that no entry exists in the state when the state is empty.
		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Float> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Float> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertFalse(state.contains(key, namespace));
				assertNull(state.get(key, namespace));
				assertEquals(Float.MIN_VALUE, state.getOrDefault(key, namespace, Float.MIN_VALUE), 0);
			}
		}

		// Adds some entries into the state and validates that their values can
		// be correctly retrieved.
		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Float> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Float> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				Float value = namespaceEntry.getValue();

				state.put(key, namespace, value);
			}
		}

		assertFalse(state.contains(null, "1"));
		assertNull(state.get(null, "1"));

		assertFalse(state.contains(1, null));
		assertNull(state.get(1, null));

		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();

			Map<String, Float> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Float> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				assertTrue(state.contains(key, namespace));

				Float expectedValue = namespaceEntry.getValue();
				assertEquals(expectedValue, state.get(key, namespace));
				assertEquals(expectedValue, state.getOrDefault(key, namespace, Float.MIN_VALUE));
			}
		}

		// Removes some values from the state and validates that the removed
		// values do not exist in the state any more.
		Map<Integer, Set<String>> removedKeyMap = new HashMap<>();

		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Set<String> removedNamespaces = new HashSet<>();

			Integer key = keyEntry.getKey();
			if (key != 0) {
				int index = 0;
				Map<String, Float> namespaceMap = keyEntry.getValue();
				for (String namespace : namespaceMap.keySet()) {
					if (index % key == 0) {
						state.remove(key, namespace);
						removedNamespaces.add(namespace);
					}
					index++;
				}
			}

			removedKeyMap.put(key, removedNamespaces);
		}

		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Set<String> removedNamespaces = removedKeyMap.get(key);

			Map<String, Float> namespaceMap = keyEntry.getValue();
			for (Map.Entry<String, Float> namespaceEntry : namespaceMap.entrySet()) {
				String namespace = namespaceEntry.getKey();
				if (removedNamespaces.contains(namespace)) {
					assertFalse(state.contains(key, namespace));
					assertNull(state.get(key, namespace));
				} else {
					Float expectedValue = namespaceEntry.getValue();
					Float actualValue = state.get(key, namespace);
					assertEquals(expectedValue, actualValue);
				}
			}

			namespaceMap.keySet().removeAll(removedNamespaces);
		}

		// Adds more entries into the state and validates that the values of the
		// pairs can be correctly retrieved.
		for (int i = 5; i < 15; ++i) {
			Map<String, Float> addedNamespaceMap = new HashMap<>();

			for (int j = 0; j < i + 5; ++j) {
				String namespace = Integer.toString(j);
				Float value = random.nextFloat();

				state.put(i, namespace, value);
				addedNamespaceMap.put(namespace, value);
			}

			Map<String, Float> namespaceMap = keyMap.get(i);
			if (namespaceMap == null) {
				keyMap.put(i, addedNamespaceMap);
			} else {
				namespaceMap.putAll(addedNamespaceMap);
			}
		}

		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, Float> expectedNamespaceMap = keyEntry.getValue();
			Map<String, Float> actualNamespaceMap = state.getAll(key);
			assertEquals(expectedNamespaceMap, actualNamespaceMap);
		}

		Map<String, Float> nullNamespaceMap = state.getAll(null);
		assertNotNull(nullNamespaceMap);
		assertTrue(nullNamespaceMap.isEmpty());

		// Removes some keys from the state and validates that there is no
		// values under these keys.
		Set<Integer> removedKeys = new HashSet<>();
		int index = 0;
		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			if (index % 4 == 0) {
				state.removeAll(key);
				removedKeys.add(key);
			}
			index++;
		}

		for (Map.Entry<Integer, Map<String, Float>> keyEntry : keyMap.entrySet()) {
			Integer key = keyEntry.getKey();
			Map<String, Float> actualNamespaceMap = state.getAll(key);
			if (removedKeys.contains(key)) {
				assertNotNull(actualNamespaceMap);
				assertTrue(actualNamespaceMap.isEmpty());
			} else {
				Map<String, Float> expectedNamespaceMap = keyEntry.getValue();
				assertEquals(expectedNamespaceMap, actualNamespaceMap);
			}
		}
	}

	@Test
	public void testIterator() {
		SubKeyedValueStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedValueStateDescriptor<>("test",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
		SubKeyedValueState<Integer, String, Float> state = backend.getSubKeyedState(descriptor);
		assertNotNull(state);

		Random random = new Random();
		Map<Integer, Map<String, Float>> keyMap = new HashMap<>();
		keyMap.put(0, Collections.singletonMap("0", null));
		for (int i = 1; i < 10; ++i) {
			Map<String, Float> namespaceMap = new HashMap<>();
			for (int j = 0; j <= i; ++j) {
				String namespace = Integer.toString(j);
				Float value = random.nextFloat();
				namespaceMap.put(namespace, value);
				state.put(i, namespace, value);
			}

			keyMap.put(i, namespaceMap);
		}

		for (int i = 1; i < 10; ++i) {
			Iterator<String> iter = state.iterator(i);

			Set<String> expectedNamespace = new HashSet<>();
			for (int j = 0; j <= i; ++j) {
				expectedNamespace.add(Integer.toString(j));
			}

			Set<String> actualNamespace = new HashSet<>();
			while (iter.hasNext()) {
				actualNamespace.add(iter.next());
			}

			assertEquals(expectedNamespace, actualNamespace);
		}

		state.remove(0, "0");
		Iterator<String> iter0 = state.iterator(0);
		assertFalse(iter0.hasNext());

		state.remove(1, "0");
		state.remove(1, "1");
		Iterator<String> iter1 = state.iterator(1);
		try {
			iter1.next();
		} catch (NoSuchElementException e) {
			// ignore this exception.
		}
		assertFalse(iter1.hasNext());

		state.put(1, "1", 1.0f);
		iter1 = state.iterator(1);
		assertTrue(iter1.hasNext());
		String namesapce = iter1.next();
		assertEquals(namesapce, "1");
		assertFalse(iter1.hasNext());

		state.removeAll(6);
		Iterator<String> iter2 = state.iterator(6);
		assertFalse(iter2.hasNext());

		state.put(6, "0", 1.0f);
		state.put(6, "1", 2.0f);
		state.put(6, "2", 3.0f);
		iter2 = state.iterator(6);
		Set<String> exceptedNamespace = new HashSet<>();
		exceptedNamespace.add("1");
		exceptedNamespace.add("2");
		Set<String> acturalNamespace = new HashSet<>();
		iter2.next();
		iter2.remove();
		while (iter2.hasNext()) {
			acturalNamespace.add(iter2.next());
		}
		assertEquals(exceptedNamespace, acturalNamespace);

		Iterator<String> iter4 = state.iterator(3);
		while (iter4.hasNext()) {
			iter4.next();
			iter4.remove();
		}

		for (int i = 0; i <= 3; ++i) {
			String namespace = Integer.toString(i);
			Float value = state.get(3, namespace);
			assertNull(value);
		}

		Iterator<String> iter5 = state.iterator(5);
		try {
			iter5.remove();
		} catch (IllegalStateException e) {
			// ignore this exception.
		}
		iter5.next();
		iter5.remove();
		try {
			iter5.remove();
		} catch (IllegalStateException e) {
			// ignore this exception.
		}
	}

	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}
}
