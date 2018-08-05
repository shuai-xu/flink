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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link MultipleKeysIndex}.
 */
public class MultipleKeysIndexTest {

	private static final Comparator<Integer> INTEGER_COMPARATOR =
		((Comparator<Integer>) (i1, i2) -> (i1 - i2));

	private static final Comparator<String> STRING_COMPARATOR =
		((Comparator<String>) (i1, i2) -> (i1.compareTo(i2)));

	@Test
	public void testInvalidArgument() {
		int numKeyColumns = 2;
		Comparator[] comparators = {INTEGER_COMPARATOR, STRING_COMPARATOR};

		// key row should have multiple columns.
		try {
			new MultipleKeysIndex(1, comparators);
			fail("Should throw IllegalArgumentException with 1 key column");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// key comparators should not be null.
		try {
			new MultipleKeysIndex(numKeyColumns, null);
			fail("Should throw IllegalArgumentException with no comparator");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// the number of comparators should be equal to the number of key columns.
		try {
			Comparator[] oneComparators = {INTEGER_COMPARATOR};
			new MultipleKeysIndex(numKeyColumns, oneComparators);
			fail("Should throw IllegalArgumentException with one key comparator");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// two key columns.
		MultipleKeysIndex keyIndex = new MultipleKeysIndex(numKeyColumns, comparators);

		// addKey with null parameter.
		try {
			keyIndex.addKey(null);
			fail("Should throw NullPointerException");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// add a key row with the different number of key columns.
		try {
			keyIndex.addKey(new Row(3));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// remove a key row with the different number of key columns.
		try {
			keyIndex.removeKey(new Row(1));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// number of prefix keys should be less than the number of total keys.
		try {
			keyIndex.firstRowKey(new Row(2));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.lastRowKey(new Row(2));
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			keyIndex.getSubIterator(new Row(2), null, null);
			fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		// if startKey or endKey is not null, the key should be sorted.
		{
			MultipleKeysIndex index = new MultipleKeysIndex(2,
				new Comparator[]{INTEGER_COMPARATOR, null});

			try {
				index.getSubIterator(Row.of(1), 1, null);
				fail("Should throw IllegalStateException");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}

			try {
				index.getSubIterator(Row.of(1), null, 1);
				fail("Should throw IllegalStateException");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}

			try {
				index.getSubIterator(Row.of(1), 1, 1);
				fail("Should throw IllegalStateException");
			} catch (Exception e) {
				assertTrue(e instanceof IllegalStateException);
			}
		}
	}

	@Test
	public void testEmptyKeyIndex() {
		// unordered keys
		MultipleKeysIndex index =
			new MultipleKeysIndex(3, new Comparator[]{null, null, null});

		assertTrue(index.getKeyIndexMap() instanceof HashMap);

		assertEquals(null, index.firstRowKey(null));
		assertEquals(null, index.lastRowKey(null));

		assertEquals(null, index.firstRowKey(Row.of(1)));
		assertEquals(null, index.lastRowKey(Row.of(2)));

		Iterator<Row> oneKeyIterator = index.getSubIterator(Row.of(1), null, null);
		assertFalse(oneKeyIterator.hasNext());

		Iterator<Row> multiKeyIterator = index.getSubIterator(null, null, null);
		assertFalse(multiKeyIterator.hasNext());

		// one ordered key
		index = new MultipleKeysIndex(3, new Comparator[]{INTEGER_COMPARATOR, null, null});

		assertTrue(index.getKeyIndexMap() instanceof TreeMap);

		assertEquals(null, index.firstRowKey(null));
		assertEquals(null, index.lastRowKey(null));

		assertEquals(null, index.firstRowKey(Row.of(1)));
		assertEquals(null, index.lastRowKey(Row.of(2)));

		oneKeyIterator = index.getSubIterator(Row.of(1), null, null);
		assertFalse(oneKeyIterator.hasNext());

		multiKeyIterator = index.getSubIterator(null, null, null);
		assertFalse(multiKeyIterator.hasNext());

		// two ordered key
		index = new MultipleKeysIndex(3,
			new Comparator[]{INTEGER_COMPARATOR, STRING_COMPARATOR, INTEGER_COMPARATOR});

		assertTrue(index.getKeyIndexMap() instanceof TreeMap);

		assertEquals(null, index.firstRowKey(null));
		assertEquals(null, index.lastRowKey(null));

		assertEquals(null, index.firstRowKey(Row.of(1)));
		assertEquals(null, index.lastRowKey(Row.of(2)));

		oneKeyIterator = index.getSubIterator(Row.of(1), null, null);
		assertFalse(oneKeyIterator.hasNext());

		multiKeyIterator = index.getSubIterator(null, null, null);
		assertFalse(multiKeyIterator.hasNext());

		multiKeyIterator = index.getSubIterator(Row.of(1), "startKey", "endKey");
		assertFalse(multiKeyIterator.hasNext());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testLastKeyIsUnordered() {
		Comparator[] comparators = {INTEGER_COMPARATOR, null, null};
		MultipleKeysIndex prefixKeysIndex = new MultipleKeysIndex(3, comparators);
		Map keyIndexMap = prefixKeysIndex.getKeyIndexMap();

		assertTrue(prefixKeysIndex.getKeyIndexMap() instanceof TreeMap);

		prefixKeysIndex.addKey(Row.of(18, 21, "31"));
		prefixKeysIndex.addKey(Row.of(18, 21, "32"));
		prefixKeysIndex.addKey(Row.of(18, 22, "31"));
		prefixKeysIndex.addKey(Row.of(18, 22, "32"));
		prefixKeysIndex.addKey(Row.of(11, 21, "31"));
		prefixKeysIndex.addKey(Row.of(11, 21, "32"));
		prefixKeysIndex.addKey(Row.of(11, 22, "31"));
		prefixKeysIndex.addKey(Row.of(11, 22, "32"));
		prefixKeysIndex.addKey(Row.of(14, 21, "31"));
		prefixKeysIndex.addKey(Row.of(14, 21, "32"));
		prefixKeysIndex.addKey(Row.of(14, 22, "31"));
		prefixKeysIndex.addKey(Row.of(14, 22, "32"));

		Set<Integer> k1Set = (Set<Integer>) keyIndexMap.keySet();
		assertArrayEquals(new Integer[] { 11, 14, 18 }, k1Set.toArray());

		for (Integer k1 : k1Set) {
			Map k2Map = (HashMap) keyIndexMap.get(k1);
			assertEquals(2, k2Map.size());
			Set<Integer> k2Set = new TreeSet<>((Set<Integer>) k2Map.keySet());
			assertArrayEquals(new Integer[] { 21, 22 }, k2Set.toArray());

			for (Integer k2 : k2Set) {
				HashSet<Row> rowSet = (HashSet<Row>) k2Map.get(k2);
				assertEquals(2, rowSet.size());
				for (Row row : rowSet) {
					assertEquals(Row.of(k1, k2, row.getField(2)), row);
				}
			}
		}

		// remove non-existing keys
		prefixKeysIndex.removeKey(Row.of(10, 21, "31"));
		prefixKeysIndex.removeKey(Row.of(18, 23, "32"));
		prefixKeysIndex.removeKey(Row.of(18, 21, "33"));
		k1Set = (Set<Integer>) keyIndexMap.keySet();
		assertArrayEquals(new Integer[] { 11, 14, 18 }, k1Set.toArray());

		for (Integer k1 : k1Set) {
			Map k2Map = (HashMap) keyIndexMap.get(k1);
			assertEquals(2, k2Map.size());
			Set<Integer> k2Set = new TreeSet<>((Set<Integer>) k2Map.keySet());
			assertArrayEquals(new Integer[] { 21, 22 }, k2Set.toArray());

			for (Integer k2 : k2Set) {
				HashSet<Row> rowSet = (HashSet<Row>) k2Map.get(k2);
				assertEquals(2, rowSet.size());
				for (Row row : rowSet) {
					assertEquals(Row.of(k1, k2, row.getField(2)), row);
				}
			}
		}

		//first,last
		//only key1 sorted
		assertEquals(11, prefixKeysIndex.firstRowKey(null).getField(0));
		assertEquals(18, prefixKeysIndex.lastRowKey(null).getField(0));

		//no existed
		assertEquals(null, prefixKeysIndex.firstRowKey(Row.of(21)));
		assertEquals(null, prefixKeysIndex.lastRowKey(Row.of(21)));

		assertEquals(null, prefixKeysIndex.firstRowKey(Row.of(11, 99)));
		assertEquals(null, prefixKeysIndex.lastRowKey(Row.of(11, 99)));

		//only key1 sorted
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(11, 21)));
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(11, 22)));
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(14, 21)));
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(14, 22)));
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(18, 21)));
		assertNotNull(prefixKeysIndex.firstRowKey(Row.of(18, 22)));

		//remove
		prefixKeysIndex.removeKey(Row.of(18, 21, "31"));
		assertEquals(2, ((Map) keyIndexMap.get(18)).size());
		assertEquals(1, ((Set) ((Map) keyIndexMap.get(18)).get(21)).size());

		assertTrue(((Set) ((Map) keyIndexMap.get(18)).get(21)).contains(Row.of(18, 21, "32")));

		prefixKeysIndex.removeKey(Row.of(18, 21, "32"));
		assertEquals(1, ((Map) keyIndexMap.get(18)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());

		prefixKeysIndex.removeKey(Row.of(18, 22, "31"));
		assertEquals(1, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());
		assertTrue(((Set) ((Map) keyIndexMap.get(18)).get(22))
			.contains(Row.of(18, 22, "32")));

		prefixKeysIndex.removeKey(Row.of(18, 22, "32"));
		//remove no existence row
		prefixKeysIndex.removeKey(Row.of(11, 21, "333"));
		assertEquals(2, ((Map) keyIndexMap).size());
		assertEquals(2, ((Map) keyIndexMap.get(11)).size());
		assertEquals(2, ((Map) keyIndexMap.get(14)).size());
	}

	@Test
	public void testLastKeyIsOrdered() {
		// TreeMap<K0,HashMap<K1,TreeSet<Row>>>
		Comparator[] comparators = {INTEGER_COMPARATOR, null, STRING_COMPARATOR};
		MultipleKeysIndex prefixKeysIndex = new MultipleKeysIndex(3, comparators);

		Map keyIndexMap = prefixKeysIndex.getKeyIndexMap();
		assertTrue(keyIndexMap instanceof TreeMap);

		prefixKeysIndex.addKey(Row.of(18, 21, "31"));
		prefixKeysIndex.addKey(Row.of(18, 21, "32"));
		prefixKeysIndex.addKey(Row.of(18, 22, "31"));
		prefixKeysIndex.addKey(Row.of(18, 22, "32"));
		prefixKeysIndex.addKey(Row.of(11, 21, "31"));
		prefixKeysIndex.addKey(Row.of(11, 21, "32"));
		prefixKeysIndex.addKey(Row.of(11, 22, "31"));
		prefixKeysIndex.addKey(Row.of(11, 22, "32"));
		prefixKeysIndex.addKey(Row.of(14, 21, "31"));
		prefixKeysIndex.addKey(Row.of(14, 21, "32"));
		prefixKeysIndex.addKey(Row.of(14, 22, "31"));
		prefixKeysIndex.addKey(Row.of(14, 22, "32"));

		Set<Integer> k1Set = (Set<Integer>) keyIndexMap.keySet();
		assertArrayEquals(new Integer[] { 11, 14, 18 }, k1Set.toArray());

		for (Integer k1 : k1Set) {
			Map k2Map = (HashMap) keyIndexMap.get(k1);
			assertEquals(2, k2Map.size());
			Set<Integer> k2Set = new TreeSet<>((Set<Integer>) k2Map.keySet());
			assertArrayEquals(new Integer[] { 21, 22 }, k2Set.toArray());

			for (Integer k2 : k2Set) {
				TreeSet<Row> rowSet = (TreeSet<Row>) k2Map.get(k2);
				assertEquals(2, rowSet.size());
				for (Row row : rowSet) {
					assertEquals(Row.of(k1, k2, row.getField(2)), row);
				}
			}
		}

		//first,last
		//only key1, key3 is sorted
		assertEquals(11, prefixKeysIndex.firstRowKey(null).getField(0));
		assertEquals("31", prefixKeysIndex.firstRowKey(null).getField(2));
		assertEquals(18, prefixKeysIndex.lastRowKey(null).getField(0));
		assertEquals("32", prefixKeysIndex.lastRowKey(null).getField(2));

		//no existed
		assertEquals(null, prefixKeysIndex.firstRowKey(Row.of(21)));
		assertEquals(null, prefixKeysIndex.lastRowKey(Row.of(21)));

		assertEquals(null, prefixKeysIndex.firstRowKey(Row.of(11, 99)));
		assertEquals(null, prefixKeysIndex.lastRowKey(Row.of(11, 99)));

		//only key1, key3 is sorted
		assertEquals(18, prefixKeysIndex.firstRowKey(Row.of(18, 21)).getField(0));
		assertEquals("31", prefixKeysIndex.firstRowKey(Row.of(18, 21)).getField(2));
		assertEquals(18, prefixKeysIndex.lastRowKey(Row.of(18, 21)).getField(0));
		assertEquals("32", prefixKeysIndex.lastRowKey(Row.of(18, 21)).getField(2));

		//remove
		prefixKeysIndex.removeKey(Row.of(18, 21, "31"));
		assertEquals(2, ((Map) keyIndexMap.get(18)).size());
		assertEquals(1, ((Set) ((Map) keyIndexMap.get(18)).get(21)).size());

		assertTrue(((Set) ((Map) keyIndexMap.get(18)).get(21))
			.contains(Row.of(18, 21, "32")));

		//only key1, key3 is sorted
		assertEquals(18, prefixKeysIndex.firstRowKey(Row.of(18, 21)).getField(0));
		assertEquals("32", prefixKeysIndex.firstRowKey(Row.of(18, 21)).getField(2));
		assertEquals(18, prefixKeysIndex.lastRowKey(Row.of(18, 21)).getField(0));
		assertEquals("32", prefixKeysIndex.lastRowKey(Row.of(18, 21)).getField(2));

		prefixKeysIndex.removeKey(Row.of(18, 21, "32"));
		assertEquals(1, ((Map) keyIndexMap.get(18)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());

		assertEquals(null, prefixKeysIndex.firstRowKey(Row.of(18, 21)));
		assertEquals(null, prefixKeysIndex.lastRowKey(Row.of(18, 21)));

		prefixKeysIndex.removeKey(Row.of(18, 22, "31"));
		assertEquals(1, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());
		assertTrue(((Set) ((Map) keyIndexMap.get(18)).get(22))
			.contains(Row.of(18, 22, "32")));

		prefixKeysIndex.removeKey(Row.of(18, 22, "32"));
		//remove no existence row
		prefixKeysIndex.removeKey(Row.of(11, 21, "333"));
		assertEquals(2, keyIndexMap.size());
		assertEquals(2, ((Map) keyIndexMap.get(11)).size());
		assertEquals(2, ((Map) keyIndexMap.get(14)).size());
	}

	@Test
	public void testPrefixKeysLastIterator() {
		//TreeMap<K1,TreeMap<K2,TreeSet<Row>>>
		Comparator[] comparators = {INTEGER_COMPARATOR, INTEGER_COMPARATOR, STRING_COMPARATOR};
		MultipleKeysIndex prefixKeysIndex = new MultipleKeysIndex(3, comparators);

		//OneLevelIterator
		Iterator<Row> oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), null, null);
		assertFalse(oneKeyIterator.hasNext());

		//multiLevelIterator
		Iterator<Row> multiKeyIterator = prefixKeysIndex.getSubIterator(null, null, null);
		assertFalse(multiKeyIterator.hasNext());

		prefixKeysIndex.addKey(Row.of(18, 21, "31"));
		prefixKeysIndex.addKey(Row.of(18, 21, "32"));
		prefixKeysIndex.addKey(Row.of(18, 22, "31"));
		prefixKeysIndex.addKey(Row.of(18, 22, "32"));
		prefixKeysIndex.addKey(Row.of(11, 21, "31"));
		prefixKeysIndex.addKey(Row.of(11, 21, "32"));
		prefixKeysIndex.addKey(Row.of(11, 22, "31"));
		prefixKeysIndex.addKey(Row.of(11, 22, "32"));
		prefixKeysIndex.addKey(Row.of(14, 21, "31"));
		prefixKeysIndex.addKey(Row.of(14, 21, "32"));
		prefixKeysIndex.addKey(Row.of(14, 22, "31"));
		prefixKeysIndex.addKey(Row.of(14, 22, "32"));

		//no range
		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), null, null);

		int count = 0;
		List<String> k3List = new ArrayList<>();
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(21, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(2, count);
		assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11), null, null);
		List<Integer> k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(11, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(null, null, null);
		List<Integer> k1List = new ArrayList<>();
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			k1List.add((Integer) key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(12, count);
		assertArrayEquals(new Integer[] { 11, 11, 11, 11, 14, 14, 14, 14, 18, 18, 18, 18 },
			k1List.toArray(new Integer[] {}));
		assertArrayEquals(new Integer[] { 21, 21, 22, 22, 21, 21, 22, 22, 21, 21, 22, 22 },
			k2List.toArray(new Integer[] {}));
		assertArrayEquals(
			new String[] { "31", "32", "31", "32", "31", "32", "31", "32", "31", "32", "31", "32" },
			k3List.toArray(new String[] {}));

		//range oneKeyIterator
		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), "32", null);
		count = 0;
		k3List.clear();
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(21, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(1, count);
		assertArrayEquals(new String[] { "32" }, k3List.toArray(new String[] {}));

		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), null, "32");
		count = 0;
		k3List.clear();
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(21, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(1, count);
		assertArrayEquals(new String[] { "31" }, k3List.toArray(new String[] {}));

		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), "0", "99");
		count = 0;
		k3List.clear();
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(21, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(2, count);
		assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));

		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), "99999", "99999999");
		assertFalse(oneKeyIterator.hasNext());

		//range multiKeyIterator
		multiKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11), 22, null);
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(22, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(2, count);
		assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11), null, 22);
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(21, key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(2, count);
		assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11), Integer.MAX_VALUE, Integer.MAX_VALUE);
		assertFalse(multiKeyIterator.hasNext());

		multiKeyIterator = prefixKeysIndex.getSubIterator(null, 18, null);
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(18, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator =
			prefixKeysIndex.getSubIterator(null, null, 14);
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(11, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(null, 14, 18);
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(14, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		multiKeyIterator = prefixKeysIndex.getSubIterator(null, 100, 101);
		assertFalse(multiKeyIterator.hasNext());

		//test iterator remove
		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), "32", null);
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals("32", key.getField(2));
			oneKeyIterator.remove();
		}

		Map keyIndexMap = prefixKeysIndex.getKeyIndexMap();
		assertEquals(1, ((Set) ((Map) keyIndexMap.get(11)).get(21)).size());

		oneKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11, 21), null, null);
		while (oneKeyIterator.hasNext()) {
			Row key = oneKeyIterator.next();
			assertEquals("31", key.getField(2));
			oneKeyIterator.remove();
		}
		assertEquals(null, ((Map) keyIndexMap.get(11)).get(21));

		assertEquals(2, ((Set) ((Map) keyIndexMap.get(11)).get(22)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(21)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(22)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(21)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());

		//test iterator remove
		multiKeyIterator = prefixKeysIndex.getSubIterator(Row.of(11), 22, null);
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(11, key.getField(0));
			assertEquals(22, key.getField(1));
			k3List.add((String) key.getField(2));
			multiKeyIterator.remove();
			count++;
		}
		assertEquals(2, count);
		assertArrayEquals(new String[] { "31", "32" }, k3List.toArray(new String[] {}));
		assertEquals(null, keyIndexMap.get(11));

		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(21)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(22)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(21)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(18)).get(22)).size());

		multiKeyIterator =
			prefixKeysIndex.getSubIterator(null, 18, null);
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(18, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			multiKeyIterator.remove();
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		assertEquals(null, keyIndexMap.get(18));

		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(21)).size());
		assertEquals(2, ((Set) ((Map) keyIndexMap.get(14)).get(22)).size());

		multiKeyIterator = prefixKeysIndex.getSubIterator(null, null, null);
		k2List = new ArrayList<>();
		k3List = new ArrayList<>();
		count = 0;
		while (multiKeyIterator.hasNext()) {
			Row key = multiKeyIterator.next();
			assertEquals(14, key.getField(0));
			k2List.add((Integer) key.getField(1));
			k3List.add((String) key.getField(2));
			multiKeyIterator.remove();
			count++;
		}
		assertEquals(4, count);
		assertArrayEquals(new Integer[] { 21, 21, 22, 22 }, k2List.toArray(new Integer[] {}));
		assertArrayEquals(new String[] { "31", "32", "31", "32" }, k3List.toArray(new String[] {}));

		assertEquals(0, keyIndexMap.size());
	}

}
