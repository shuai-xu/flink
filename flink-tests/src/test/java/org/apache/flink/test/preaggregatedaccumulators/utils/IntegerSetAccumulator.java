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

package org.apache.flink.test.preaggregatedaccumulators.utils;

import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * An accumulator who contains a set of integers to be filtered out.
 */
public class IntegerSetAccumulator implements Accumulator<Integer, IntegerSetAccumulator.IntegerSet> {
	private IntegerSet integerSet = new IntegerSet();

	public IntegerSetAccumulator() { }

	@Override
	public void add(Integer value) {
		integerSet.getIntegers().add(value);
	}

	@Override
	public IntegerSet getLocalValue() {
		return integerSet;
	}

	@Override
	public void resetLocal() {
		integerSet.getIntegers().clear();
	}

	@Override
	public void merge(Accumulator<Integer, IntegerSetAccumulator.IntegerSet> other) {
		integerSet.getIntegers().addAll(other.getLocalValue().getIntegers());
	}

	@Override
	public Accumulator<Integer, IntegerSet> clone() {
		Accumulator<Integer, IntegerSet> cloned = new IntegerSetAccumulator();
		cloned.merge(this);
		return cloned;
	}

	/**
	 * Serializable wrapper class for a set of integers.
	 */
	static class IntegerSet implements Serializable {
		private Set<Integer> integers = new HashSet<>();

		Set<Integer> getIntegers() {
			return integers;
		}
	}
}


