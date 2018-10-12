/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operator.join.batch;

import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.sort.RecordComparator;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * Gets elements from left or right {{BinaryExternalSorter}}. Return elements with smaller
 * keys first.
 */
public class SortMergeCoTableValuedAggIterator implements Closeable {

	private final Projection<BinaryRow, BinaryRow> leftProjection;
	private final Projection<BinaryRow, BinaryRow> rightProjection;
	private final RecordComparator keyComparator;
	private final MutableObjectIterator<BinaryRow> leftIterator;
	private final MutableObjectIterator<BinaryRow> rightIterator;

	private BinaryRow leftRow;
	private BinaryRow leftKey;
	private BinaryRow rightRow;
	private BinaryRow rightKey;
	private BinaryRow returnRow;
	private BinaryRow returnKey;

	public SortMergeCoTableValuedAggIterator(
			BinaryRowSerializer leftSerializer,
			BinaryRowSerializer rightSerializer,
			Projection leftProjection,
			Projection rightProjection,
			RecordComparator keyComparator,
			MutableObjectIterator<BinaryRow> leftIterator,
			MutableObjectIterator<BinaryRow> rightIterator) throws IOException {
		this.leftProjection = leftProjection;
		this.rightProjection = rightProjection;
		this.keyComparator = keyComparator;
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;

		this.leftRow = leftSerializer.createInstance();
		this.rightRow = rightSerializer.createInstance();
		head();
	}


	/**
	 * Get next element from left sorter.
	 */
	public boolean nextLeft() throws IOException {
		if ((leftRow = leftIterator.next(leftRow)) != null) {
			leftKey = leftProjection.apply(leftRow);
			return true;
		} else {
			leftRow = null;
			leftKey = null;
			return false;
		}
	}

	/**
	 * Get next element from right sorter.
	 */
	public boolean nextRight() throws IOException {
		if ((rightRow = rightIterator.next(rightRow)) != null) {
			rightKey = rightProjection.apply(rightRow);
			return true;
		} else {
			rightRow = null;
			rightKey = null;
			return false;
		}
	}

	public void head() throws IOException {
		nextLeft();
		nextRight();
	}

	public BinaryRow getElement() throws IOException {
		return returnRow;
	}

	public BinaryRow getKey() throws IOException {
		return returnKey;
	}

	/**
	 * Get the next element, i.e, the next smallest element.
	 *
	 * @return 0 if there are no elements, 1 if return left element, 2 if return right element.
	 * @throws IOException
	 */
	public int getNextSmallest() throws IOException {

		// there are no data
		if (leftRow == null && rightRow == null) {
			return 0;
		}

		// only right contains data
		if (leftRow == null) {
			returnRow = rightRow;
			returnKey = rightKey;
			return 2;
		}
		// only left contains data
		else if (rightRow == null) {
			returnRow = leftRow;
			returnKey = leftKey;
			return 1;
		} else {
			int cmp = keyComparator.compare(leftKey, rightKey);
			// return left if left key is small
			if (cmp < 0) {
				returnRow = leftRow;
				returnKey = leftKey;
				return 1;
			} else {
				returnRow = rightRow;
				returnKey = rightKey;
				return 2;
			}
		}
	}

	@Override
	public void close() { }
}
