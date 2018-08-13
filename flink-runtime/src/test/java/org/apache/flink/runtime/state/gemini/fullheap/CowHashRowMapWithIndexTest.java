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
import org.apache.flink.runtime.state.gemini.RowMap;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CowHashRowMapWithIndex}.
 */
public class CowHashRowMapWithIndexTest {

	private static final Comparator<Integer> INTEGER_COMPARATOR =
		((Comparator<Integer>) (i1, i2) -> (i1 - i2));

	private static final Comparator<String> STRING_COMPARATOR =
		((Comparator<String>) (i1, i2) -> (i1.compareTo(i2)));

	@Test
	public void testIndexCreate() {
		// one unsorted key.
		Comparator[] oneUnsortedComparators = {null};
		RowMap dataRowMap = mock(RowMap.class);
		try {
			new CowHashRowMapWithIndex(1, oneUnsortedComparators, dataRowMap);
			fail("Should throw exceptions because the key is only one.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalStateException);
		}

		// one sorted key.
		Comparator[] oneSortedComparators = {INTEGER_COMPARATOR};
		CowHashRowMapWithIndex cowHashRowMapWithIndex =
			new CowHashRowMapWithIndex(1, oneSortedComparators, dataRowMap);
		CowPrefixKeyIndex prefixKeyIndex = cowHashRowMapWithIndex.getPrefixKeyIndex();
		assertTrue(prefixKeyIndex instanceof OneKeyIndex);

		// two keys, both are sorted.
		Comparator[] twoSortedComparators = {INTEGER_COMPARATOR, INTEGER_COMPARATOR};
		cowHashRowMapWithIndex =
			new CowHashRowMapWithIndex(2, twoSortedComparators, dataRowMap);
		prefixKeyIndex = cowHashRowMapWithIndex.getPrefixKeyIndex();
		assertTrue(prefixKeyIndex instanceof MultipleKeysIndex);
	}


	@Test
	public void testNormal() {
		// 3 columns of key Row: K0(Integer, sorted), K1(Integer, unsorted), K2(String, sorted)
		// 1 column of value Row: V0(String)
		// nested map for index: TreeMap<K0, HashMap<K1, TreeSet<Row.of(K0, K1, K2)>>>
		Comparator[] comparators = {INTEGER_COMPARATOR, null, STRING_COMPARATOR};
		RowMap dataRowMap = mock(RowMap.class);
		CowHashRowMapWithIndex cowHashRowMapWithIndex =
			new CowHashRowMapWithIndex(3, comparators, dataRowMap);

		// null
		assertEquals(null, cowHashRowMapWithIndex.firstPair(null));
		assertEquals(null, cowHashRowMapWithIndex.lastPair(null));

		// add one
		cowHashRowMapWithIndex.put(Row.of(18, 21, "31"), Row.of("row_18_21_31"));

		// when dataRowMap null
		when(dataRowMap.get(Row.of(18, 21, "31"))).thenReturn(null);
		assertEquals(null, cowHashRowMapWithIndex.firstPair(null));
		assertEquals(null, cowHashRowMapWithIndex.lastPair(null));

		// when dataRowMap has one
		when(dataRowMap.get(Row.of(18, 21, "31"))).thenReturn(Row.of("row_18_21_31"));
		assertEquals(Row.of(18, 21, "31"), cowHashRowMapWithIndex.firstPair(null).getKey());
		assertEquals(Row.of("row_18_21_31"), cowHashRowMapWithIndex.firstPair(null).getValue());
		assertEquals(Row.of(18, 21, "31"), cowHashRowMapWithIndex.lastPair(null).getKey());
		assertEquals(Row.of("row_18_21_31"), cowHashRowMapWithIndex.lastPair(null).getValue());

		// add more
		cowHashRowMapWithIndex.put(Row.of(11, 21, "31"), Row.of("row_11_21_31"));
		cowHashRowMapWithIndex.put(Row.of(11, 21, "32"), Row.of("row_11_21_32"));
		cowHashRowMapWithIndex.put(Row.of(11, 21, "33"), Row.of("row_11_21_33"));
		when(dataRowMap.get(Row.of(11, 21, "31"))).thenReturn(Row.of("row_11_21_31"));
		when(dataRowMap.get(Row.of(11, 21, "32"))).thenReturn(Row.of("row_11_21_32"));
		when(dataRowMap.get(Row.of(11, 21, "33"))).thenReturn(Row.of("row_11_21_33"));

		assertEquals(Row.of(11, 21, "31"), cowHashRowMapWithIndex.firstPair(null).getKey());
		assertEquals(Row.of("row_11_21_31"), cowHashRowMapWithIndex.firstPair(null).getValue());
		assertEquals(Row.of(18, 21, "31"), cowHashRowMapWithIndex.lastPair(null).getKey());
		assertEquals(Row.of("row_18_21_31"), cowHashRowMapWithIndex.lastPair(null).getValue());

		assertEquals(Row.of(11, 21, "31"), cowHashRowMapWithIndex.firstPair(Row.of(11)).getKey());
		assertEquals(Row.of("row_11_21_31"), cowHashRowMapWithIndex.firstPair(Row.of(11)).getValue());
		assertEquals(Row.of(11, 21, "33"), cowHashRowMapWithIndex.lastPair(Row.of(11)).getKey());
		assertEquals(Row.of("row_11_21_33"), cowHashRowMapWithIndex.lastPair(Row.of(11)).getValue());

		assertEquals(Row.of(11, 21, "31"), cowHashRowMapWithIndex.firstPair(Row.of(11, 21)).getKey());
		assertEquals(Row.of("row_11_21_31"), cowHashRowMapWithIndex.firstPair(Row.of(11)).getValue());
		assertEquals(Row.of(11, 21, "33"), cowHashRowMapWithIndex.lastPair(Row.of(11, 21)).getKey());
		assertEquals(Row.of("row_11_21_33"), cowHashRowMapWithIndex.lastPair(Row.of(11, 21)).getValue());

		//remove last
		when(dataRowMap.remove(Row.of(11, 21, "33"))).thenReturn(Row.of("row_11_21_33"));
		assertEquals(Row.of("row_11_21_33"), cowHashRowMapWithIndex.remove(Row.of(11, 21, "33")));

		assertEquals(Row.of(11, 21, "31"), cowHashRowMapWithIndex.firstPair(Row.of(11, 21)).getKey());
		assertEquals(Row.of("row_11_21_31"), cowHashRowMapWithIndex.firstPair(Row.of(11)).getValue());
		assertEquals(Row.of(11, 21, "32"), cowHashRowMapWithIndex.lastPair(Row.of(11, 21)).getKey());
		assertEquals(Row.of("row_11_21_32"), cowHashRowMapWithIndex.lastPair(Row.of(11, 21)).getValue());

		//get one
		assertEquals(null, cowHashRowMapWithIndex.get(Row.of(11, 21, "NONE")));
		assertEquals(Row.of("row_11_21_31"), cowHashRowMapWithIndex.get(Row.of(11, 21, "31")));
	}

	@Test
	public void testIterator() {
		// 3 columns of key Row: K0(Integer, sorted), K1(Integer, unsorted), K2(String, sorted)
		// 1 column of value Row: V0(String)
		// nested map for index: TreeMap<K0, HashMap<K1, TreeSet<Row.of(K0, K1, K2)>>>
		Comparator[] comparators = {INTEGER_COMPARATOR, null, STRING_COMPARATOR};
		RowMap dataRowMap = mock(RowMap.class);
		CowHashRowMapWithIndex cowHashRowMapWithIndex =
			new CowHashRowMapWithIndex(3, comparators, dataRowMap);

		Iterator<Pair<Row, Row>> iter = cowHashRowMapWithIndex.getIterator(Row.of(18));
		assertFalse(iter.hasNext());

		when(dataRowMap.get(any(Row.class))).thenReturn(null);

		cowHashRowMapWithIndex.put(Row.of(18, 21, "31"), Row.of("row_18_21_31"));
		cowHashRowMapWithIndex.put(Row.of(18, 21, "32"), Row.of("row_18_21_32"));
		cowHashRowMapWithIndex.put(Row.of(18, 22, "33"), Row.of("row_18_22_33"));
		cowHashRowMapWithIndex.put(Row.of(18, 22, "34"), Row.of("row_18_22_34"));
		cowHashRowMapWithIndex.put(Row.of(19, 23, "35"), Row.of("row_19_23_35"));

		when(dataRowMap.get(Row.of(18, 21, "31"))).thenReturn(Row.of("row_18_21_31"));
		when(dataRowMap.get(Row.of(18, 21, "32"))).thenReturn(Row.of("row_18_21_32"));
		when(dataRowMap.get(Row.of(18, 22, "33"))).thenReturn(Row.of("row_18_22_33"));
		when(dataRowMap.get(Row.of(18, 22, "34"))).thenReturn(Row.of("row_18_22_34"));
		when(dataRowMap.get(Row.of(19, 23, "35"))).thenReturn(Row.of("row_19_23_35"));

		Map keyIndexMap = ((MultipleKeysIndex) cowHashRowMapWithIndex.getPrefixKeyIndex()).getKeyIndexMap();
		assertEquals(2,	((Map) keyIndexMap.get(18)).size());

		iter = cowHashRowMapWithIndex.getIterator(Row.of(18));
		List<Integer> k2List = new ArrayList<>();
		List<String> k3List = new ArrayList<>();
		List<String> valueList = new ArrayList<>();
		int count = 0;
		while (iter.hasNext()) {
			Pair<Row, Row> pair = iter.next();
			assertEquals(18, pair.getKey().getField(0));
			k2List.add((Integer) pair.getKey().getField(1));
			k3List.add((String) pair.getKey().getField(2));
			valueList.add((String) pair.getValue().getField(0));
			count++;
		}

		assertEquals(4, count);
		Assert.assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		Assert.assertArrayEquals(new String[] { "31", "32", "33", "34" }, k3List.toArray(new String[] {}));
		Assert.assertArrayEquals(
			new String[] { "row_18_21_31", "row_18_21_32", "row_18_22_33", "row_18_22_34" },
			valueList.toArray(new String[] {}));

		//2.
		iter = cowHashRowMapWithIndex.getIterator(Row.of(18, 21));
		k3List = new ArrayList<>();
		valueList = new ArrayList<>();
		count = 0;
		while (iter.hasNext()) {
			Pair<Row, Row> pair = iter.next();
			assertEquals(18, pair.getKey().getField(0));
			assertEquals(21, pair.getKey().getField(1));
			k3List.add((String) pair.getKey().getField(2));
			valueList.add((String) pair.getValue().getField(0));
			count++;
		}

		assertEquals(2, count);
		Assert.assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));
		Assert.assertArrayEquals(new String[] { "row_18_21_31", "row_18_21_32" }, valueList.toArray(new String[] {}));

		//3.
		iter = cowHashRowMapWithIndex.getSubIterator(Row.of(18, 21),"32", null);
		k3List = new ArrayList<>();
		valueList = new ArrayList<>();
		count = 0;
		while (iter.hasNext()) {
			Pair<Row, Row> pair = iter.next();
			assertEquals(18, pair.getKey().getField(0));
			assertEquals(21, pair.getKey().getField(1));
			k3List.add((String) pair.getKey().getField(2));
			valueList.add((String) pair.getValue().getField(0));
			count++;
			iter.remove();
		}

		assertEquals(1, count);
		Assert.assertArrayEquals(new String[] { "32" }, k3List.toArray(new String[] {}));
		Assert.assertArrayEquals(new String[] { "row_18_21_32" }, valueList.toArray(new String[] {}));

		iter = cowHashRowMapWithIndex.getIterator(Row.of(18, 21));
		k3List = new ArrayList<>();
		valueList = new ArrayList<>();
		count = 0;
		while (iter.hasNext()) {
			Pair<Row, Row> pair = iter.next();
			assertEquals(18, pair.getKey().getField(0));
			assertEquals(21, pair.getKey().getField(1));
			k3List.add((String) pair.getKey().getField(2));
			valueList.add((String) pair.getValue().getField(0));
			count++;
			iter.remove();
		}

		assertEquals(1, count);
		Assert.assertArrayEquals(new String[] { "31" }, k3List.toArray(new String[] {}));
		Assert.assertArrayEquals(new String[] { "row_18_21_31" }, valueList.toArray(new String[] {}));

		assertEquals(1, ((Map) keyIndexMap.get(18)).size());

		iter = cowHashRowMapWithIndex.getIterator(Row.of(18, 21));
		assertFalse(iter.hasNext());

		iter = cowHashRowMapWithIndex.getSubIterator(null, 18, null);
		Assert.assertTrue(iter.hasNext());

		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());

		assertEquals(1, ((Map) keyIndexMap.get(19)).size());

		iter = cowHashRowMapWithIndex.getSubIterator(null, 19, null);
		count = 0;
		while (iter.hasNext()) {
			Pair<Row, Row> pair = iter.next();
			assertEquals(19, pair.getKey().getField(0));
			assertEquals(23, pair.getKey().getField(1));
			assertEquals("35", pair.getKey().getField(2));
			assertEquals("row_19_23_35", pair.getValue().getField(0));
			count++;
			iter.remove();
		}
		assertEquals(1, count);

		assertEquals(null, keyIndexMap.get(19));
	}

	@Test
	public void testPairOperation() {
		// use ordered key to ensure the index is built
		Comparator[] comparators = {INTEGER_COMPARATOR, INTEGER_COMPARATOR};
		RowMap dataRowMap = new MockRowMap();
		CowHashRowMapWithIndex cowHashRowMapWithIndex =
			new CowHashRowMapWithIndex(2, comparators, dataRowMap);

		Random random = new Random(System.currentTimeMillis());
		Map<Row, Row> referenceMap = new HashMap<>();
		for (int i = 0; i < 10000; i++) {
			Row key = Row.of(random.nextInt(), random.nextInt());
			Row value = Row.of(random.nextInt());
			cowHashRowMapWithIndex.put(key, value);
			referenceMap.put(key, value);
		}
		assertEquals(referenceMap.size(), cowHashRowMapWithIndex.size());

		Iterator<Pair<Row, Row>> iterator = cowHashRowMapWithIndex.getIterator(null);
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			assertEquals(referenceMap.get(pair.getKey()), pair.getValue());
		}

		iterator = cowHashRowMapWithIndex.getIterator(null);
		Set<Row> removedKey = new HashSet<>();
		Set<Row> setValueKey = new HashSet<>();
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			Row key = pair.getKey();

			boolean op = random.nextBoolean();
			if (op) {
				removedKey.add(key);
				referenceMap.remove(key);
				iterator.remove();
			} else {
				setValueKey.add(key);
				Row newValue = Row.of(random.nextInt());
				pair.setValue(newValue);
				referenceMap.put(key, newValue);
			}
		}

		assertEquals(referenceMap.size(), cowHashRowMapWithIndex.size());
		iterator = cowHashRowMapWithIndex.getIterator(null);
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			assertEquals(referenceMap.get(pair.getKey()), pair.getValue());
		}

		for (Row key : removedKey) {
			assertNull(cowHashRowMapWithIndex.get(key));
		}

		for (Row key : setValueKey) {
			assertEquals(cowHashRowMapWithIndex.get(key), referenceMap.get(key));
		}

		cowHashRowMapWithIndex.put(Row.of(1, 1), Row.of(2));
		iterator = cowHashRowMapWithIndex.getIterator(null);
		while (iterator.hasNext()) {
			Pair<Row, Row> pair = iterator.next();
			iterator.remove();
			try {
				pair.setValue(Row.of(1));
				fail("Should throw IllegalStateException");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}
		}
	}

	private class MockRowMap implements RowMap {

		private Map<Row, Row> data;

		MockRowMap() {
			data = new HashMap<>();
		}

		@Override
		public int size() {
			return data.size();
		}

		@Override
		public boolean isEmpty() {
			return data.isEmpty();
		}

		@Override
		public Row get(Row key) {
			if (key == null) {
				return null;
			}
			return data.get(key);
		}

		@Override
		public Row put(Row key, Row value) {
			if (key == null) {
				return null;
			}

			return data.put(key, value);
		}

		@Override
		public Row remove(Row key) {
			if (key == null) {
				return null;
			}

			return data.remove(key);
		}

		@Override
		public Iterator<Pair<Row, Row>> getIterator(Row prefixKeys) {
			throw new UnsupportedOperationException();
		};

		@Override
		public <K> Iterator<Pair<Row, Row>> getSubIterator(Row prefixKeys, K startKey, K endKey) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Pair<Row, Row> firstPair(Row prefixKeys) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Pair<Row, Row> lastPair(Row prefixKeys) {
			throw new UnsupportedOperationException();
		}

		@Override
		public RowMapSnapshot createSnapshot() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void releaseSnapshot(RowMapSnapshot snapshot) {
			throw new UnsupportedOperationException();
		}
	}
}
