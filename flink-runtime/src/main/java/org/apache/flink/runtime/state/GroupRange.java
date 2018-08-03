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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A special implementation of {@link GroupSet} which is composed of contiguous
 * groups. Each range is defined by an inclusive lower bound and an exclusive
 * upper bound. Those ranges with same bounds will be considered empty.
 */
public class GroupRange implements GroupSet {
	private static final long serialVersionUID = 1L;

	/** The lower bound of the range (inclusive). */
	private final int startGroup;

	/** The upper bound of the range (exclusive). */
	private final int endGroup;

	/** A singleton instance for empty ranges. */
	public static final GroupRange EMPTY = new GroupRange(0, 0);

	/**
	 * Constructs a new group range with given bounds.
	 *
	 * @param startGroup The start group of the range.
	 * @param endGroup The end group of the range.
	 */
	public GroupRange(int startGroup, int endGroup) {
		Preconditions.checkArgument(startGroup >= 0);
		Preconditions.checkArgument(endGroup >= 0);

		this.startGroup = startGroup;
		this.endGroup = endGroup;
	}

	//--------------------------------------------------------------------------

	/**
	 * Returns the start group in the range.
	 *
	 * @return The start group in the range.
	 */
	public int getStartGroup() {
		return startGroup;
	}

	/**
	 * Returns the end group in the range.
	 *
	 * @return The end group in the range.
	 */
	public int getEndGroup() {
		return endGroup;
	}

	/**
	 * A helper method to create a group range with given bounds.
	 *
	 * @param startGroup The lower bound of the range.
	 * @param endGroup The upper bound of the range.
	 * @return The group range with given bounds.
	 */
	public static GroupRange of(int startGroup, int endGroup) {
		return new GroupRange(startGroup, endGroup);
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean isEmpty() {
		return startGroup >= endGroup;
	}

	@Override
	public int getNumGroups() {
		return isEmpty() ? 0 : endGroup - startGroup;
	}

	@Override
	public boolean contains(int group) {
		return group >= startGroup && group < endGroup;
	}

	@Override
	public GroupSet intersect(GroupSet otherSet) {
		Preconditions.checkNotNull(otherSet);

		if (!(otherSet instanceof GroupRange)) {
			throw new UnsupportedOperationException();
		}

		GroupRange otherRange = (GroupRange) otherSet;

		int intersectStartGroup = Math.max(startGroup, otherRange.startGroup);
		int intersectEndGroup = Math.min(endGroup, otherRange.endGroup);
		return new GroupRange(intersectStartGroup, intersectEndGroup);
	}

	@Override
	public Iterator<Integer> iterator() {
		return isEmpty() ? Collections.emptyIterator() : new GroupIterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		GroupRange other = (GroupRange) o;

		return startGroup == other.startGroup &&
			endGroup == other.endGroup;
	}

	@Override
	public int hashCode() {
		int result = startGroup;
		result = 31 * result + endGroup;
		return result;
	}

	@Override
	public String toString() {
		return "GroupRange{" +
			"startGroup=" + startGroup +
			", endGroup=" + endGroup +
		"}";
	}

	//--------------------------------------------------------------------------

	/**
	 * A helper class to iterate the groups in the range.
	 */
	private class GroupIterator implements Iterator<Integer> {

		private int currentGroup;

		GroupIterator() {
			this.currentGroup = startGroup;
		}

		@Override
		public boolean hasNext() {
			return (currentGroup < endGroup);
		}

		@Override
		public Integer next() {
			if (currentGroup >= endGroup) {
				throw new NoSuchElementException();
			}

			int result = currentGroup;
			currentGroup++;
			return result;
		}
	}
}

