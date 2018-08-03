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

import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.types.Pair;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Iterator;

/**
 * An iterator over the pairs in the state.
 */
public class SortedPrefixPartitionIterator extends AbstractSortedPartitionIterator<Pair<Row, Row>> {

	private Comparator<Object> comparator;
	private int keyField;

	public SortedPrefixPartitionIterator(Collection<Iterator<Pair<Row, Row>>> groupIterators, Comparator<Object> comparator, int keyField) {
		super(groupIterators);
		this.comparator = Preconditions.checkNotNull(comparator);
		this.keyField = Preconditions.checkNotNull(keyField);
	}

	@Override
	public int compareKeys(Row keyA, Row keyB) {
		return comparator.compare(keyA.getField(keyField), keyB.getField(keyField));
	}

	@Override
	protected Pair<Row, Row> getValueFromPair(Pair<Row, Row> pair) {
		return pair;
	}
}
