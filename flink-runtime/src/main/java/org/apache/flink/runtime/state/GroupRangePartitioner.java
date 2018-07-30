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

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A helper class to partition {@link GroupRange}s.
 *
 * <p>All splits will have roughly the same number of groups. In the cases where
 * the groups cannot be evenly partitioned, the heading partitions will take an
 * extra group.
 *
 * <p>If the number of groups in this range is less than the number of
 * partitions, empty partitions will be contained in the result.
 */
public class GroupRangePartitioner {

	/**
	 * Partitions the given range into the given number of splits.
	 *
	 * @param groupRange The range to be partitioned.
	 * @param numPartitions The number of partitions.
	 * @return The ranges of the resulting partitions.
	 */
	public static List<GroupRange> partition(GroupRange groupRange, int numPartitions) {
		Preconditions.checkArgument(groupRange != null && !groupRange.isEmpty());
		Preconditions.checkArgument(numPartitions > 0);

		List<GroupRange> partitionGroupRanges = new ArrayList<>(numPartitions);

		int numGroupsPerPartition = groupRange.getNumGroups() / numPartitions;
		int numFatPartitions = groupRange.getNumGroups() % numPartitions;

		int startGroupForThisPartition = groupRange.getStartGroup();
		for (int partition = 0; partition < numPartitions; ++partition) {

			int numGroupsForThisPartition =
				partition < numFatPartitions ? numGroupsPerPartition + 1 : numGroupsPerPartition;

			GroupRange groupRangeForThisPartition =
				new GroupRange(
					startGroupForThisPartition,
					startGroupForThisPartition + numGroupsForThisPartition
				);
			partitionGroupRanges.add(groupRangeForThisPartition);

			startGroupForThisPartition += numGroupsForThisPartition;
		}

		return partitionGroupRanges;
	}

	/**
	 * Returns the partition to which the given group belongs when partitioning
	 * the given range into the given number of partitions.
	 *
	 * @param groupRange The range to be partitioned.
	 * @param numPartitions The number of partitions.
	 * @param group The group whose assignment is to be retrieved.
	 *
	 */
	public static int getPartitionIndex(GroupRange groupRange, int numPartitions, int group) {
		Preconditions.checkArgument(groupRange != null && !groupRange.isEmpty());
		Preconditions.checkArgument(numPartitions > 0);
		Preconditions.checkArgument(groupRange.contains(group));

		int startGroup = groupRange.getStartGroup();
		int numGroupsPerPartition = groupRange.getNumGroups() / numPartitions;
		int numFatPartitions = groupRange.getNumGroups() % numPartitions;
		int numGroupsInFatPartitions = numFatPartitions * (numGroupsPerPartition + 1);

		if (group < startGroup + numGroupsInFatPartitions) {
			return (group - startGroup) / (numGroupsPerPartition + 1);
		} else {
			return (group - startGroup - numGroupsInFatPartitions) / numGroupsPerPartition + numFatPartitions;
		}
	}

	/**
	 * Returns the groups assigned to the given partition when partitioning the
	 * given range into the given number of partitions.
	 *
	 * @param groupRange The range to be partitioned.
	 * @param numPartitions The number of partitions.
	 * @param partition The partition whose range is to be retrieved.
	 * @return The groups assigned to the given partition.
	 */
	public static GroupRange getPartitionRange(GroupRange groupRange, int numPartitions, int partition) {
		Preconditions.checkArgument(groupRange != null && !groupRange.isEmpty());
		Preconditions.checkArgument(numPartitions > 0);
		Preconditions.checkArgument(partition >= 0 && partition < numPartitions);

		int startGroup = groupRange.getStartGroup();
		int numGroupsPerPartition = groupRange.getNumGroups() / numPartitions;
		int numFatPartitions = groupRange.getNumGroups() % numPartitions;

		int startGroupForThisPartition =
			partition < numFatPartitions ?
				startGroup + partition * numGroupsPerPartition + partition :
				startGroup + partition * numGroupsPerPartition + numFatPartitions;

		int endGroupForThisPartition =
			partition < numFatPartitions ?
				startGroupForThisPartition + numGroupsPerPartition + 1 :
				startGroupForThisPartition + numGroupsPerPartition;

		return new GroupRange(startGroupForThisPartition, endGroupForThisPartition);
	}
}

