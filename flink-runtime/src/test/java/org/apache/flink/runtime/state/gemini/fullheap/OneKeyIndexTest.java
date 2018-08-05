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
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link OneKeyIndex}.
 */
public class OneKeyIndexTest {

	private static final Comparator<Integer> INTEGER_COMPARATOR =
		((Comparator<Integer>) (i1, i2) -> (i1 - i2));

	@Test
	public void testInvalidArgument() {
		// constructor
		try {
			new OneKeyIndex(null);
			fail("Should throw NullPointerException for the constructor with null comparator");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		OneKeyIndex keyIndex = new OneKeyIndex(INTEGER_COMPARATOR);

		try {
			keyIndex.addKey(null);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			keyIndex.addKey(new Row(0));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.removeKey(new Row(0));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.firstRowKey(Row.of(1));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.lastRowKey(Row.of(1));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.getSubIterator(Row.of(1), null, null);
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testIndexAccess() {
		// 1 columns of key Row: K0(Integer, sorted)
		OneKeyIndex prefixKeyIndex = new OneKeyIndex(INTEGER_COMPARATOR);

		assertEquals(null, prefixKeyIndex.firstRowKey(null));
		assertEquals(null, prefixKeyIndex.lastRowKey(null));

		Iterator<Row> iter = prefixKeyIndex.getSubIterator(null, null, null);
		assertFalse(iter.hasNext());

		iter = prefixKeyIndex.getSubIterator(null, 2, null);
		assertFalse(iter.hasNext());

		iter = prefixKeyIndex.getSubIterator(null, null, 2);
		assertFalse(iter.hasNext());

		prefixKeyIndex.addKey(Row.of(1));
		prefixKeyIndex.addKey(Row.of(2));
		prefixKeyIndex.addKey(Row.of(3));
		assertEquals(Row.of(1), prefixKeyIndex.firstRowKey(null));
		assertEquals(Row.of(3), prefixKeyIndex.lastRowKey(null));

		iter = prefixKeyIndex.getSubIterator(null, null, null);
		int i = 0;
		while (iter.hasNext()) {
			i++;
			assertEquals(Row.of(i), iter.next());
		}
		assertEquals(3, i);

		iter = prefixKeyIndex.getSubIterator(null, 2, null);
		i = 0;
		while (iter.hasNext()) {
			i++;
			assertEquals(Row.of(i + 1), iter.next());
		}
		assertEquals(2, i);

		iter = prefixKeyIndex.getSubIterator(null, null, 2);
		i = 0;
		while (iter.hasNext()) {
			i++;
			assertEquals(Row.of(1), iter.next());
			iter.remove();
		}
		assertEquals(1, i);

		iter = prefixKeyIndex.getSubIterator(null, 1, 4);
		i = 0;
		while (iter.hasNext()) {
			i++;
			assertEquals(Row.of(i + 1), iter.next());
		}
		assertEquals(2, i);

		assertEquals(Row.of(2), prefixKeyIndex.firstRowKey(null));
		assertEquals(Row.of(3), prefixKeyIndex.lastRowKey(null));

		prefixKeyIndex.removeKey(Row.of(3));
		assertEquals(Row.of(2), prefixKeyIndex.firstRowKey(null));
		assertEquals(Row.of(2), prefixKeyIndex.lastRowKey(null));

		prefixKeyIndex.removeKey(Row.of(2));
		assertEquals(null, prefixKeyIndex.firstRowKey(null));
		assertEquals(null, prefixKeyIndex.lastRowKey(null));
	}

}
