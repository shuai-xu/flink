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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link GroupRangePartitioner}.
 */
public class GroupRangePartitionerTest {

	@Test
	public void testPartitionBalance() {
		GroupRange groupRange = new GroupRange(20, 120);

		List<GroupRange> partitionGroupRanges = GroupRangePartitioner.partition(groupRange, 5);
		assertEquals(5, partitionGroupRanges.size());
		assertEquals(new GroupRange(20, 40), partitionGroupRanges.get(0));
		assertEquals(new GroupRange(40, 60), partitionGroupRanges.get(1));
		assertEquals(new GroupRange(60, 80), partitionGroupRanges.get(2));
		assertEquals(new GroupRange(80, 100), partitionGroupRanges.get(3));
		assertEquals(new GroupRange(100, 120), partitionGroupRanges.get(4));

		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 20));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 30));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 40));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 50));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 60));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 70));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 80));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 90));
		assertEquals(4, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 100));
		assertEquals(4, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 110));

		assertEquals(new GroupRange(20, 40), GroupRangePartitioner.getPartitionRange(groupRange, 5, 0));
		assertEquals(new GroupRange(40, 60), GroupRangePartitioner.getPartitionRange(groupRange, 5, 1));
		assertEquals(new GroupRange(60, 80), GroupRangePartitioner.getPartitionRange(groupRange, 5, 2));
		assertEquals(new GroupRange(80, 100), GroupRangePartitioner.getPartitionRange(groupRange, 5, 3));
		assertEquals(new GroupRange(100, 120), GroupRangePartitioner.getPartitionRange(groupRange, 5, 4));
	}

	@Test
	public void testPartitionImbalance() {
		GroupRange groupRange = new GroupRange(20, 123);

		List<GroupRange> partitionGroupRanges = GroupRangePartitioner.partition(groupRange, 5);
		assertEquals(5, partitionGroupRanges.size());
		assertEquals(new GroupRange(20, 41), partitionGroupRanges.get(0));
		assertEquals(new GroupRange(41, 62), partitionGroupRanges.get(1));
		assertEquals(new GroupRange(62, 83), partitionGroupRanges.get(2));
		assertEquals(new GroupRange(83, 103), partitionGroupRanges.get(3));
		assertEquals(new GroupRange(103, 123), partitionGroupRanges.get(4));

		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 20));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 30));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 40));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 41));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 50));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 61));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 62));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 70));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 82));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 83));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 90));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 102));
		assertEquals(4, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 103));
		assertEquals(4, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 110));
		assertEquals(4, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 122));

		assertEquals(new GroupRange(20, 41), GroupRangePartitioner.getPartitionRange(groupRange, 5, 0));
		assertEquals(new GroupRange(41, 62), GroupRangePartitioner.getPartitionRange(groupRange, 5, 1));
		assertEquals(new GroupRange(62, 83), GroupRangePartitioner.getPartitionRange(groupRange, 5, 2));
		assertEquals(new GroupRange(83, 103), GroupRangePartitioner.getPartitionRange(groupRange, 5, 3));
		assertEquals(new GroupRange(103, 123), GroupRangePartitioner.getPartitionRange(groupRange, 5, 4));
	}

	@Test
	public void testPartitionShort() {
		GroupRange groupRange = new GroupRange(20, 24);

		List<GroupRange> partitionGroupRanges = GroupRangePartitioner.partition(groupRange, 5);
		assertEquals(5, partitionGroupRanges.size());
		assertEquals(new GroupRange(20, 21), partitionGroupRanges.get(0));
		assertEquals(new GroupRange(21, 22), partitionGroupRanges.get(1));
		assertEquals(new GroupRange(22, 23), partitionGroupRanges.get(2));
		assertEquals(new GroupRange(23, 24), partitionGroupRanges.get(3));
		assertEquals(new GroupRange(24, 24), partitionGroupRanges.get(4));

		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 20));
		assertEquals(1, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 21));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 22));
		assertEquals(3, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 23));

		assertEquals(new GroupRange(20, 21), GroupRangePartitioner.getPartitionRange(groupRange, 5, 0));
		assertEquals(new GroupRange(21, 22), GroupRangePartitioner.getPartitionRange(groupRange, 5, 1));
		assertEquals(new GroupRange(22, 23), GroupRangePartitioner.getPartitionRange(groupRange, 5, 2));
		assertEquals(new GroupRange(23, 24), GroupRangePartitioner.getPartitionRange(groupRange, 5, 3));
		assertEquals(new GroupRange(24, 24), GroupRangePartitioner.getPartitionRange(groupRange, 5, 4));
	}

	@Test
	public void testPartitionExceed() {
		GroupRange groupRange = new GroupRange(0, Integer.MAX_VALUE);

		List<GroupRange> partitionGroupRanges = GroupRangePartitioner.partition(groupRange, 5);
		assertEquals(5, partitionGroupRanges.size());
		assertEquals(new GroupRange(0, 429496730), partitionGroupRanges.get(0));
		assertEquals(new GroupRange(429496730, 858993460), partitionGroupRanges.get(1));
		assertEquals(new GroupRange(858993460, 1288490189), partitionGroupRanges.get(2));
		assertEquals(new GroupRange(1288490189, 1717986918), partitionGroupRanges.get(3));
		assertEquals(new GroupRange(1717986918, Integer.MAX_VALUE), partitionGroupRanges.get(4));

		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 1));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 22));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 333));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 4444));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 55555));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 666666));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 7777777));
		assertEquals(0, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 88888888));
		assertEquals(2, GroupRangePartitioner.getPartitionIndex(groupRange, 5, 999999999));

		assertEquals(new GroupRange(0, 429496730), GroupRangePartitioner.getPartitionRange(groupRange, 5, 0));
		assertEquals(new GroupRange(429496730, 858993460), GroupRangePartitioner.getPartitionRange(groupRange, 5, 1));
		assertEquals(new GroupRange(858993460, 1288490189), GroupRangePartitioner.getPartitionRange(groupRange, 5, 2));
		assertEquals(new GroupRange(1288490189, 1717986918), GroupRangePartitioner.getPartitionRange(groupRange, 5, 3));
		assertEquals(new GroupRange(1717986918, Integer.MAX_VALUE), GroupRangePartitioner.getPartitionRange(groupRange, 5, 4));
	}
}

