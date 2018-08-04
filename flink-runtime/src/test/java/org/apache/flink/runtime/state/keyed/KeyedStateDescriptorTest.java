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

package org.apache.flink.runtime.state.keyed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link KeyedStateDescriptor}s.
 */
public class KeyedStateDescriptorTest {

	private static final Merger<String> TEST_MERGER = new Merger<String>() {
		private static final long serialVersionUID = -871406401408513162L;

		@Override
		public String merge(String value1, String value2) {
			return value1 + value2;
		}

		@Override
		public boolean equals(Object obj) {
			return (obj == this) || (obj != null && obj.getClass() == getClass());
		}
	};

	@Test
	public void testKeyedValueStateDescriptor() throws Exception {

		try {
			new KeyedValueStateDescriptor<>(null, IntSerializer.INSTANCE,
				StringSerializer.INSTANCE);
			fail("Should throw exceptions because the state name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedValueStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedValueStateDescriptor<>("testName", IntSerializer.INSTANCE,
				(TypeSerializer<String>) null);
			fail("Should throw exceptions because the value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedValueStateDescriptor<>(null,
				IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the state name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedValueStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedValueStateDescriptor<>("testName",
				IntSerializer.INSTANCE,
				null, TEST_MERGER);
			fail("Should throw exceptions because the value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		TypeSerializer<String> serializer =
			new KryoSerializer<>(String.class, new ExecutionConfig());

		KeyedValueStateDescriptor<Integer, String> descriptor =
			new KeyedValueStateDescriptor<>("testName",  IntSerializer.INSTANCE, serializer, TEST_MERGER);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(serializer, descriptor.getValueSerializer());
		assertEquals(TEST_MERGER, descriptor.getValueMerger());

		KeyedValueStateDescriptor<Integer, String> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testKeyedListStateDescriptor() throws Exception {

		try {
			new KeyedListStateDescriptor<>(null, IntSerializer.INSTANCE,
				StringSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedListStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedListStateDescriptor<>("testName", IntSerializer.INSTANCE,
				(TypeSerializer<String>) null);
			fail("Should throw exceptions because the element serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedListStateDescriptor<>(null,
				(TypeSerializer<Integer>) null, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedListStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedListStateDescriptor<>("testName",
				IntSerializer.INSTANCE, (TypeSerializer<String>) null);
			fail("Should throw exceptions because the element serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		TypeSerializer<String> elementSerializer =
			new KryoSerializer<>(String.class, new ExecutionConfig());

		KeyedListStateDescriptor<Integer, String> descriptor =
			new KeyedListStateDescriptor<>("testName",  IntSerializer.INSTANCE, elementSerializer);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(elementSerializer, descriptor.getElementSerializer());
		assertNotNull(descriptor.getValueMerger());

		KeyedListStateDescriptor<Integer, String> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testKeyedMapStateDescriptor() throws Exception {

		try {
			new KeyedMapStateDescriptor<>(null, (TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				(TypeSerializer<String>) null, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, (TypeSerializer<Long>) null);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>(null,
				(TypeSerializer<Integer>) null,
				LongSerializer.INSTANCE, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null,
				LongSerializer.INSTANCE, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE,
				(TypeSerializer<Long>) null, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE, null, TEST_MERGER);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE,
				LongSerializer.INSTANCE, StringSerializer.INSTANCE, null);
			fail("Should throw exceptions because the map value merger is null in the local state.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeSerializer<Long> mapKeySerializer =
			new KryoSerializer<>(Long.class, executionConfig);
		TypeSerializer<String> mapValueSerializer =
			new KryoSerializer<>(String.class, executionConfig);

		KeyedMapStateDescriptor<Integer, Long, String> descriptor =
			new KeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, mapKeySerializer, mapValueSerializer, TEST_MERGER);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(mapKeySerializer, descriptor.getMapKeySerializer());
		assertEquals(mapValueSerializer, descriptor.getMapValueSerializer());
		assertEquals(TEST_MERGER, descriptor.getMapValueMerger());

		KeyedMapStateDescriptor<Integer, Long, String> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testKeyedSortedMapStateDescriptor() throws Exception {

		Comparator<Long> comparator = new NaturalComparator<>();

		try {
			new KeyedSortedMapStateDescriptor<>(null, IntSerializer.INSTANCE,
				comparator, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				comparator, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				null, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				comparator, null, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				comparator, LongSerializer.INSTANCE, (TypeSerializer<String>) null);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>(null, IntSerializer.INSTANCE,
				comparator, LongSerializer.INSTANCE, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				comparator, LongSerializer.INSTANCE, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName",  IntSerializer.INSTANCE,
				null, LongSerializer.INSTANCE, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				comparator, null, StringSerializer.INSTANCE, TEST_MERGER);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				comparator, LongSerializer.INSTANCE, null, TEST_MERGER);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new KeyedSortedMapStateDescriptor<>("testName", IntSerializer.INSTANCE,
				comparator, LongSerializer.INSTANCE, StringSerializer.INSTANCE, null);
			fail("Should throw exceptions because the map value merger is null in the local state.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeSerializer<Long> mapKeySerializer =
			new KryoSerializer<>(Long.class, executionConfig);
		TypeSerializer<String> mapValueSerializer =
			new KryoSerializer<>(String.class, executionConfig);

		KeyedSortedMapStateDescriptor<Integer, Long, String> descriptor =
			new KeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, comparator, mapKeySerializer, mapValueSerializer, TEST_MERGER);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(comparator, descriptor.getMapKeyComparator());
		assertEquals(mapKeySerializer, descriptor.getMapKeySerializer());
		assertEquals(mapValueSerializer, descriptor.getMapValueSerializer());
		assertEquals(TEST_MERGER, descriptor.getMapValueMerger());

		KeyedSortedMapStateDescriptor<Integer, Long, String> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}
}

