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

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link GroupRange}.
 */
public class GroupRangeTest {

	@Test
	public void testInvalidConstructArguments() {
		try {
			new GroupRange(-3, 1);
			fail("Should throw an IllegalArgumentException because the start group is negative.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}

		try {
			new GroupRange(1, -3);
			fail("Should throw an IllegalArgumentException because the end group is negative.");
		} catch (Exception e) {
			assertTrue(e instanceof IllegalArgumentException);
		}
	}

	@Test
	public void testAccessNormal1() throws Exception {
		GroupRange groupRange = new GroupRange(12, 34);
		assertEquals(12, groupRange.getStartGroup());
		assertEquals(34, groupRange.getEndGroup());
		assertFalse(groupRange.isEmpty());
		assertEquals(22, groupRange.getNumGroups());
		assertTrue(groupRange.contains(12));
		assertTrue(groupRange.contains(30));
		assertFalse(groupRange.contains(34));

		GroupRange groupRangeCopy = CommonTestUtils.createCopySerializable(groupRange);
		assertEquals(groupRange, groupRangeCopy);
		assertEquals(groupRange.hashCode(), groupRangeCopy.hashCode());
	}

	@Test
	public void testAccessNormal2() throws Exception {
		GroupRange groupRange = new GroupRange(0, Integer.MAX_VALUE);
		assertEquals(0, groupRange.getStartGroup());
		assertEquals(Integer.MAX_VALUE, groupRange.getEndGroup());
		assertFalse(groupRange.isEmpty());
		assertEquals(Integer.MAX_VALUE, groupRange.getNumGroups());
		assertTrue(groupRange.contains(0));
		assertFalse(groupRange.contains(Integer.MAX_VALUE));

		GroupRange groupRangeCopy = CommonTestUtils.createCopySerializable(groupRange);
		assertEquals(groupRange, groupRangeCopy);
		assertEquals(groupRange.hashCode(), groupRangeCopy.hashCode());
	}

	@Test
	public void testAccessEmpty1() throws Exception {
		GroupRange groupRange = new GroupRange(3, 3);
		assertEquals(3, groupRange.getStartGroup());
		assertEquals(3, groupRange.getEndGroup());
		assertTrue(groupRange.isEmpty());
		assertEquals(0, groupRange.getNumGroups());
		assertFalse(groupRange.contains(3));

		GroupRange groupRangeCopy = CommonTestUtils.createCopySerializable(groupRange);
		assertEquals(groupRange, groupRangeCopy);
		assertEquals(groupRange.hashCode(), groupRangeCopy.hashCode());
	}

	@Test
	public void testAccessEmpty2() throws Exception {
		GroupRange groupRange = new GroupRange(8, 4);
		assertEquals(8, groupRange.getStartGroup());
		assertEquals(4, groupRange.getEndGroup());
		assertTrue(groupRange.isEmpty());
		assertEquals(0, groupRange.getNumGroups());
		assertFalse(groupRange.contains(8));
		assertFalse(groupRange.contains(6));
		assertFalse(groupRange.contains(4));

		GroupRange groupRangeCopy = CommonTestUtils.createCopySerializable(groupRange);
		assertEquals(groupRange, groupRangeCopy);
		assertEquals(groupRange.hashCode(), groupRangeCopy.hashCode());
	}

	@Test
	public void testExpiredIterator() {
		GroupRange groupRange = new GroupRange(0, 3);

		Iterator<Integer> iterator = groupRange.iterator();
		for (int i = groupRange.getStartGroup(); i < groupRange.getEndGroup(); ++i) {
			assertTrue(iterator.hasNext());
			assertEquals(i, (int) iterator.next());
		}

		assertFalse(iterator.hasNext());

		try {
			iterator.next();
			fail("Should throw an NoSuchElementException because the iterator has already expired.");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchElementException);
		}
	}

	@Test
	public void testIntersect() {
		GroupRange baseRange = new GroupRange(30, 60);

		GroupRange testRange1 = new GroupRange(10, 10);
		assertEquals(new GroupRange(30, 10), baseRange.intersect(testRange1));

		GroupRange testRange2 = new GroupRange(10, 20);
		assertEquals(new GroupRange(30, 20), baseRange.intersect(testRange2));

		GroupRange testRange3 = new GroupRange(10, 30);
		assertEquals(new GroupRange(30, 30), baseRange.intersect(testRange3));

		GroupRange testRange4 = new GroupRange(10, 40);
		assertEquals(new GroupRange(30, 40), baseRange.intersect(testRange4));

		GroupRange testRange5 = new GroupRange(10, 60);
		assertEquals(new GroupRange(30, 60), baseRange.intersect(testRange5));

		GroupRange testRange6 = new GroupRange(10, 70);
		assertEquals(new GroupRange(30, 60), baseRange.intersect(testRange6));

		GroupRange testRange7 = new GroupRange(30, 30);
		assertEquals(new GroupRange(30, 30), baseRange.intersect(testRange7));

		GroupRange testRange8 = new GroupRange(30, 40);
		assertEquals(new GroupRange(30, 40), baseRange.intersect(testRange8));

		GroupRange testRange9 = new GroupRange(30, 60);
		assertEquals(new GroupRange(30, 60), baseRange.intersect(testRange9));

		GroupRange testRange10 = new GroupRange(30, 70);
		assertEquals(new GroupRange(30, 60), baseRange.intersect(testRange10));

		GroupRange testRange11 = new GroupRange(40, 40);
		assertEquals(new GroupRange(40, 40), baseRange.intersect(testRange11));

		GroupRange testRange12 = new GroupRange(40, 50);
		assertEquals(new GroupRange(40, 50), baseRange.intersect(testRange12));

		GroupRange testRange13 = new GroupRange(40, 60);
		assertEquals(new GroupRange(40, 60), baseRange.intersect(testRange13));

		GroupRange testRange14 = new GroupRange(40, 70);
		assertEquals(new GroupRange(40, 60), baseRange.intersect(testRange14));

		GroupRange testRange15 = new GroupRange(60, 60);
		assertEquals(new GroupRange(60, 60), baseRange.intersect(testRange15));

		GroupRange testRange16 = new GroupRange(60, 70);
		assertEquals(new GroupRange(60, 60), baseRange.intersect(testRange16));

		GroupRange testRange17 = new GroupRange(70, 80);
		assertEquals(new GroupRange(70, 60), baseRange.intersect(testRange17));

		GroupRange testRange18 = new GroupRange(70, 10);
		assertEquals(new GroupRange(70, 10), baseRange.intersect(testRange18));
	}
}

