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
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An unordered iterator over all the values in the internal state.
 *
 * @param <T> The type of the values in the internal state.
 */
public abstract class AbstractPartitionIterator<T> implements Iterator<T> {

	/** An iterator over the group iterators of the internal state. */
	private final Iterator<Iterator<Pair<Row, Row>>> groupIteratorIterator;

	/** The iterator over the pairs in the current group. */
	private volatile Iterator<Pair<Row, Row>> currentGroupIterator;

	public AbstractPartitionIterator(Collection<Iterator<Pair<Row, Row>>> groupIterators) {
		Preconditions.checkNotNull(groupIterators);

		this.groupIteratorIterator = groupIterators.iterator();
		do {
			this.currentGroupIterator = groupIteratorIterator.hasNext() ?
				groupIteratorIterator.next() :
				Collections.emptyIterator();
		} while (!currentGroupIterator.hasNext() && groupIteratorIterator.hasNext());
	}

	@Override
	public boolean hasNext() {
		if (currentGroupIterator.hasNext()) {
			return true;
		} else {
			while (!currentGroupIterator.hasNext() && groupIteratorIterator.hasNext()) {
				this.currentGroupIterator = groupIteratorIterator.hasNext() ?
					groupIteratorIterator.next() :
					Collections.emptyIterator();
			}
			return currentGroupIterator.hasNext();
		}
	}

	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		Pair<Row, Row> pair = currentGroupIterator.next();
		return getValueFromPair(pair);
	}

	@Override
	public void remove() {
		currentGroupIterator.remove();
	}

	/**
	 * Gets the value decoded from the given pair.
	 *
	 * @param pair The pair in the state.
	 * @return The value decoded from the given pair.
	 */
	protected abstract T getValueFromPair(Pair<Row, Row> pair);
}
