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

import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A sorted iterator over the values in the internal state. The pairs corresponding to the
 * values are already sorted in each group in the internal state. The results of the group
 * iterators are merged to obtain the total order of the values.
 *
 * @param <T> The type of the values in the internal state.
 */
public abstract class AbstractSortedPartitionIterator<T> implements Iterator<T> {

	/** The nodes maintaining the iterating state of all groups. */
	private PriorityQueue<GroupIterateNode> groupNodes;

	/** The node for the current head group. */
	private volatile GroupIterateNode headGroupNode;

	private final Collection<Iterator<Pair<Row, Row>>> groupIterators;
	/**
	 * Constructor with the iterators over the pairs in the groups.
	 *
	 * @param groupIterators The iterators over the pairs in the groups.
	 */
	public AbstractSortedPartitionIterator(Collection<Iterator<Pair<Row, Row>>> groupIterators) {
		Preconditions.checkNotNull(groupIterators);

		this.headGroupNode = null;
		this.groupIterators = groupIterators;
		this.groupNodes = null;
	}

	private void initGroupNodes() {
		this.groupNodes = new PriorityQueue<>();
		for (Iterator<Pair<Row, Row>> groupIterator : groupIterators) {
			if (groupIterator.hasNext()) {
				GroupIterateNode groupNode = new GroupIterateNode(groupIterator);
				groupNodes.add(groupNode);
			}
		}
	}

	/**
	 * Compares the given two keys for order. The result of this method must be
	 * consistent with the ordering of the keys in the state.
	 *
	 * @param keyA The first key to be compared.
	 * @param keyB The second key to be compared.
	 * @return A negative, zero, or a positive integer as the first key is less
	 *         than, equal to, or greater than the second key.
	 */
	protected abstract int compareKeys(Row keyA, Row keyB);

	/**
	 * Gets the value decoded from the given pair.
	 *
	 * @param pair The pair in the state.
	 * @return The value decoded from the given pair.
	 */
	protected abstract T getValueFromPair(Pair<Row, Row> pair);

	@Override
	public boolean hasNext() {
		if (groupNodes == null) {
			initGroupNodes();
		}
		return (!groupNodes.isEmpty() ||
			(headGroupNode != null && headGroupNode.hasNext()));
	}

	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		// move the iterator for the head group to the next pair and add the
		// group back into the priority queue to find the new head group.
		if (headGroupNode != null && headGroupNode.hasNext()) {
			headGroupNode.next();
			groupNodes.add(headGroupNode);
		}

		headGroupNode = groupNodes.poll();

		Pair<Row, Row> headPair = headGroupNode.getCurrentPair();
		return getValueFromPair(headPair);
	}

	@Override
	public void remove() {
		if (headGroupNode == null) {
			throw new IllegalStateException();
		}

		headGroupNode.remove();
	}

	/**
	 * A helper class to maintain the state of the iterating over the pairs in
	 * a group.
	 */
	private class GroupIterateNode implements Comparable<GroupIterateNode> {

		/** The iterator over the pairs in the group. */
		private final Iterator<Pair<Row, Row>> iterator;

		/** The current pair pointed by the iterator. */
		private volatile Pair<Row, Row> currentPair;

		private GroupIterateNode(Iterator<Pair<Row, Row>> iterator) {
			Preconditions.checkState(iterator != null && iterator.hasNext());

			this.iterator = iterator;
			this.currentPair = iterator.next();
		}

		Pair<Row, Row> getCurrentPair() {
			return currentPair;
		}

		boolean hasNext() {
			return iterator.hasNext();
		}

		void next() {
			currentPair = iterator.next();
		}

		void remove() {
			iterator.remove();
		}

		@SuppressWarnings("unchecked")
		@Override
		public int compareTo(GroupIterateNode that) {
			return compareKeys(currentPair.getKey(), that.currentPair.getKey());
		}
	}
}
