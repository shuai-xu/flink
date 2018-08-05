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
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Implementation of {@link CowPrefixKeyIndex} where the {@link Row} of key has
 * only one column and the column must be ordered.
 */
class OneKeyIndex implements CowPrefixKeyIndex {

	/**
	 * Use {@link TreeSet} to build key index.
	 */
	private final TreeSet<Row> keyIndex;

	@SuppressWarnings("unchecked")
	public OneKeyIndex(Comparator comparator) {
		Preconditions.checkNotNull(comparator);

		this.keyIndex = new TreeSet<>((row1, row2) ->
			comparator.compare(row1.getField(0), row2.getField(0)));
	}

	@Override
	public void addKey(Row key) {
		Preconditions.checkNotNull(key);
		Preconditions.checkArgument(key.getArity() == 1);
		keyIndex.add(key);
	}

	@Override
	public void removeKey(Row key) {
		if (key == null) {
			return;
		}

		Preconditions.checkArgument(key.getArity() == 1);
		keyIndex.remove(key);
	}

	@Override
	public Row firstRowKey(Row prefixKey) {
		checkPrefixKey(prefixKey);
		return keyIndex.isEmpty() ? null : keyIndex.first();
	}

	@Override
	public Row lastRowKey(Row prefixKey) {
		checkPrefixKey(prefixKey);
		return keyIndex.isEmpty() ? null : keyIndex.last();
	}

	@Override
	public <K> Iterator<Row> getSubIterator(Row prefixKey, K startKey, K endKey) {
		checkPrefixKey(prefixKey);
		if (startKey == null && endKey == null) {
			return keyIndex.iterator();
		} else if (startKey == null) {
			return keyIndex.headSet(Row.of(endKey)).iterator();
		} else if (endKey == null) {
			return keyIndex.tailSet(Row.of(startKey)).iterator();
		} else {
			return keyIndex.subSet(Row.of(startKey), Row.of(endKey)).iterator();
		}
	}

	private void checkPrefixKey(Row prefixKey) {
		Preconditions.checkArgument(prefixKey == null || prefixKey.getArity() == 0);
	}

}
