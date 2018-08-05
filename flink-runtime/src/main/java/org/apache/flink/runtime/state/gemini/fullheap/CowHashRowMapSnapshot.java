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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.gemini.RowMapSnapshot;
import org.apache.flink.types.DefaultPair;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link RowMapSnapshot} for {@link CowHashRowMap}.
 */
public class CowHashRowMapSnapshot implements RowMapSnapshot {

	/**
	 * The row map this snapshot belongs to.
	 */
	private final CowHashRowMap cowHashRowMap;

	/**
	 * The copy of array tables in row map.
	 */
	private CowHashRowMap.CowHashRowMapEntry[] snapshotTable;

	/**
	 * The number of key-value mappings in snapshot.
	 */
	private final int snapshotSize;

	/**
	 * The version of this snapshot.
	 */
	private final int snapshotVersion;

	/**
	 * Offsets for the individual groups. This is lazily created when the snapshot
	 * is grouped by group during the process of writing this snapshot to an output
	 * as part of checkpointing.
	 */
	private int[] groupOffsets;

	/**
	 * Constructor for {@link CowHashRowMapSnapshot}.
	 *
	 * @param cowHashRowMap The row map this snapshot belongs to.
	 * @param snapshotTable The copy of array tables in row map.
	 * @param snapshotSize The number of key-value mappings in snapshot.
	 * @param snapshotVersion The version of this snapshot.
	 */
	CowHashRowMapSnapshot(
		CowHashRowMap cowHashRowMap,
		CowHashRowMap.CowHashRowMapEntry[] snapshotTable,
		int snapshotSize,
		int snapshotVersion) {
		this.cowHashRowMap = Preconditions.checkNotNull(cowHashRowMap);
		this.snapshotTable = Preconditions.checkNotNull(snapshotTable);
		this.snapshotSize = snapshotSize;
		this.snapshotVersion = snapshotVersion;
	}

	@Override
	public int size() {
		return snapshotSize;
	}

	public int snapshotVersion() {
		return snapshotVersion;
	}

	@Override
	public Iterator<Pair<Row, Row>> groupIterator(int group) {
		GroupRange groupRange = (GroupRange) cowHashRowMap.getStateBackend().getGroups();
		if (!groupRange.contains(group)) {
			return Collections.emptyIterator();
		}

		partitionRowByGroup();

		int effectiveIdx = group - groupRange.getStartGroup();
		int startOffset = effectiveIdx == 0 ? 0 : groupOffsets[effectiveIdx - 1];
		int endOffset = groupOffsets[effectiveIdx];
		return new SnapshotGroupIterator(startOffset, endOffset);
	}

	@Override
	public void releaseSnapshot() {
		snapshotTable = null;
		cowHashRowMap.releaseSnapshot(this);
	}

	@VisibleForTesting
	public CowHashRowMap.CowHashRowMapEntry[] getSnapshotTable() {
		return snapshotTable;
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void partitionRowByGroup() {
		// We only have to perform this step once before the first group is written
		if (groupOffsets != null) {
			return;
		}

		int totalGroups = cowHashRowMap.getStateBackend().getNumGroups();
		GroupRange groups = (GroupRange) cowHashRowMap.getStateBackend().getGroups();
		int startGroup = groups.getStartGroup();

		int[] histogram = new int[groups.getNumGroups() + 1];

		Partitioner<Row> partitioner = cowHashRowMap.getDescriptor().getPartitioner();

		CowHashRowMap.CowHashRowMapEntry[] unfold = new CowHashRowMap.CowHashRowMapEntry[snapshotSize];

		// In this step we
		// i) 'unfold' the linked list of entries to a flat array
		// ii) build a histogram for groups
		int unfoldIndex = 0;
		for (CowHashRowMap.CowHashRowMapEntry entry : snapshotTable) {
			while (null != entry) {
				int effectiveIdx = partitioner.partition(entry.key, totalGroups) - startGroup + 1;
				++histogram[effectiveIdx];
				unfold[unfoldIndex++] = entry;
				entry = entry.next;
			}
		}

		// 2) We accumulate the histogram bins to obtain group ranges in the final array
		for (int i = 1; i < histogram.length; ++i) {
			histogram[i] += histogram[i - 1];
		}

		// 3) We repartition the entries by key-group, using the histogram values as write indexes.
		// We have made sure that the snapshotTable is big enough to hold the flattened entries in
		// CowHashRowMap#snapshotTable(), we can safely reuse it as the destination array here.
		for (CowHashRowMap.CowHashRowMapEntry entry : unfold) {
			int effectiveIdx = partitioner.partition(entry.key, totalGroups) - startGroup;
			snapshotTable[histogram[effectiveIdx]++] = entry;
		}

		// 4) As byproduct, we also created the key-group offsets
		this.groupOffsets = histogram;
	}

	// -----------------------------------------------------------------------------------------------------------------

	class SnapshotGroupIterator implements Iterator<Pair<Row, Row>> {

		private int startOffset;

		private int endOffset;

		SnapshotGroupIterator(int startOffset, int endOffset) {
			this.startOffset = startOffset;
			this.endOffset = endOffset;
		}

		@Override
		public boolean hasNext() {
			return startOffset < endOffset;
		}

		@Override
		public Pair<Row, Row> next() {

			if (!hasNext()) {
				throw new NoSuchElementException();
			}

			CowHashRowMap.CowHashRowMapEntry entry = snapshotTable[startOffset];
			startOffset++;
			return new DefaultPair<>(entry.getKey(), entry.getValue());
		}
	}
}
