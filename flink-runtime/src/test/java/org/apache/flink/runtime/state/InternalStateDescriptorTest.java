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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.Merger;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link InternalStateDescriptor}.
 */
public class InternalStateDescriptorTest {

	private static final Comparator<Integer> TEST_COMPARATOR =
			((Comparator<Integer>) (i1, i2) -> (i1 - i2));

	private static final Partitioner<Row> TEST_PARTITIONER =
			((Partitioner<Row>) (row, num) -> (row.hashCode() % num));

	private static final Merger<Path> TEST_MERGER =
			((Merger<Path>) (r1, r2) -> r1);

	@Test
	public void testInvalidColumnArguments() {

		try {
			new InternalColumnDescriptor<>(null, IntSerializer.INSTANCE, TEST_COMPARATOR);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalColumnDescriptor<>("name", null, TEST_COMPARATOR);
			fail("Should throw exceptions because the serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalColumnDescriptor<>("name",  IntSerializer.INSTANCE, (Comparator<Integer>) null);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalColumnDescriptor<>("name",  IntSerializer.INSTANCE, (Merger<Integer>) null);
			fail("Should throw exceptions because the merger is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}
	}

	@Test
	public void testInvalidStateArguments() {

		InternalColumnDescriptor<Integer> keyColumnDescriptor1 =
			new InternalColumnDescriptor<>("key", IntSerializer.INSTANCE, TEST_COMPARATOR);
		InternalColumnDescriptor<String> keyColumnDescriptor2 =
			new InternalColumnDescriptor<>("key2", StringSerializer.INSTANCE);

		List<InternalColumnDescriptor<?>> keyColumnDescriptors = new ArrayList<>();
		keyColumnDescriptors.add(keyColumnDescriptor1);
		keyColumnDescriptors.add(keyColumnDescriptor2);

		InternalColumnDescriptor<Path> valueColumnDescriptor =
			new InternalColumnDescriptor<>("value", new KryoSerializer<>(Path.class, new ExecutionConfig()));

		List<InternalColumnDescriptor<?>> valueColumnDescriptors =
			Collections.singletonList(valueColumnDescriptor);

		try {
			new InternalStateDescriptor(null, TEST_PARTITIONER, keyColumnDescriptors, valueColumnDescriptors);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalStateDescriptor("name", null, keyColumnDescriptors, valueColumnDescriptors);
			fail("Should throw exceptions because the partitioner is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalStateDescriptor("name", TEST_PARTITIONER, null, valueColumnDescriptors);
			fail("Should throw exceptions because the descriptors for key columns is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new InternalStateDescriptor("name", TEST_PARTITIONER, keyColumnDescriptors, null);
			fail("Should throw exceptions because the descriptors for value columns is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			List<InternalColumnDescriptor<?>> nullKeyColumnDescriptors =
				new ArrayList<>(keyColumnDescriptors);
			nullKeyColumnDescriptors.add(null);

			new InternalStateDescriptor("name", TEST_PARTITIONER, nullKeyColumnDescriptors, valueColumnDescriptors);
			fail("Should throw exceptions because a descriptor for key columns is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			List<InternalColumnDescriptor<?>> nullValueColumnDescriptors =
				new ArrayList<>(valueColumnDescriptors);
			nullValueColumnDescriptors.add(null);

			new InternalStateDescriptor("name", TEST_PARTITIONER, keyColumnDescriptors, nullValueColumnDescriptors);
			fail("Should throw exceptions because a descriptor for value columns is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			List<InternalColumnDescriptor<?>> duplicateKeyColumnDescriptors =
				new ArrayList<>(keyColumnDescriptors);
			duplicateKeyColumnDescriptors.add(keyColumnDescriptor2);

			new InternalStateDescriptor("name", TEST_PARTITIONER, duplicateKeyColumnDescriptors, valueColumnDescriptors);
			fail("Should throw exceptions because there exist duplicated columns.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			List<InternalColumnDescriptor<?>> duplicateKeyColumnDescriptors =
				new ArrayList<>(keyColumnDescriptors);
			duplicateKeyColumnDescriptors.add(valueColumnDescriptor);

			new InternalStateDescriptor("name", TEST_PARTITIONER, duplicateKeyColumnDescriptors, valueColumnDescriptors);
			fail("Should throw exceptions because there exist duplicated columns.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			List<InternalColumnDescriptor<?>> duplicateValueColumnDescriptors =
				new ArrayList<>(valueColumnDescriptors);
			duplicateValueColumnDescriptors.add(keyColumnDescriptor2);

			new InternalStateDescriptor("name", TEST_PARTITIONER, keyColumnDescriptors, duplicateValueColumnDescriptors);
			fail("Should throw exceptions because there exist duplicated columns.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			List<InternalColumnDescriptor<?>> duplicateValueColumnDescriptors =
				new ArrayList<>(valueColumnDescriptors);
			duplicateValueColumnDescriptors.add(valueColumnDescriptor);

			new InternalStateDescriptor("name", TEST_PARTITIONER, keyColumnDescriptors, duplicateValueColumnDescriptors);
			fail("Should throw exceptions because there exist duplicated columns.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testStateDescriptorBuilder() {
		InternalStateDescriptorBuilder builder = new InternalStateDescriptorBuilder("name");

		Partitioner<Row> partitioner = (Partitioner<Row>) ((row, num) -> row.hashCode() % num);

		InternalStateDescriptor descriptor = builder
			.addKeyColumn("key1", IntSerializer.INSTANCE, TEST_COMPARATOR)
			.addKeyColumn("key2", StringSerializer.INSTANCE)
			.addValueColumn("value", new KryoSerializer<>(Path.class, new ExecutionConfig()), TEST_MERGER)
			.setPartitioner(partitioner)
			.getDescriptor();

		assertNotNull(descriptor);
		assertEquals("name", descriptor.getName());
		assertEquals(partitioner, descriptor.getPartitioner());

		List<InternalColumnDescriptor<?>> keyColumnDescriptors = descriptor.getKeyColumnDescriptors();
		assertNotNull(keyColumnDescriptors);
		assertEquals(2, keyColumnDescriptors.size());
		assertEquals(2, descriptor.getNumKeyColumns());

		InternalColumnDescriptor<?> keyColumnDescriptor1 = descriptor.getKeyColumnDescriptor(0);
		assertEquals("key1", keyColumnDescriptor1.getName());
		assertEquals(IntSerializer.INSTANCE, keyColumnDescriptor1.getSerializer());
		assertEquals(TEST_COMPARATOR, keyColumnDescriptor1.getComparator());
		assertTrue(keyColumnDescriptor1.isOrdered());

		InternalColumnDescriptor<?> keyColumnDescriptor2 = descriptor.getKeyColumnDescriptor(1);
		assertEquals("key2", keyColumnDescriptor2.getName());
		assertEquals(StringSerializer.INSTANCE, keyColumnDescriptor2.getSerializer());
		assertNull(keyColumnDescriptor2.getComparator());
		assertFalse(keyColumnDescriptor2.isOrdered());

		RowSerializer keySerializer = descriptor.getKeySerializer();
		TypeSerializer<?>[] keyColumnSerializers = keySerializer.getFieldSerializers();
		assertEquals(keyColumnDescriptor2.getSerializer(), keyColumnSerializers[1]);

		List<InternalColumnDescriptor<?>> valueColumnDescriptors = descriptor.getValueColumnDescriptors();
		assertNotNull(valueColumnDescriptors);
		assertEquals(1, valueColumnDescriptors.size());
		assertEquals(1, descriptor.getNumValueColumns());

		InternalColumnDescriptor<?> valueColumnDescriptor = descriptor.getValueColumnDescriptor(0);
		assertEquals("value", valueColumnDescriptor.getName());
		assertTrue(valueColumnDescriptor.getSerializer() instanceof KryoSerializer);

		RowSerializer valueSerializer = descriptor.getValueSerializer();
		TypeSerializer<?>[] valueColumnSerializers = valueSerializer.getFieldSerializers();
		assertEquals(valueColumnDescriptor.getSerializer(), valueColumnSerializers[0]);
	}
}

