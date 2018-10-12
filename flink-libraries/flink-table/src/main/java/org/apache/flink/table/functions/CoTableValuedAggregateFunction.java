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

package org.apache.flink.table.functions;

import org.apache.flink.util.Collector;

/**
 * Base class for User-Defined co-table-valued Aggregates.
 *
 * <p>The behavior of an {@link CoTableValuedAggregateFunction} can be defined by implementing
 * a series of custom methods.
 * An {@link CoTableValuedAggregateFunction} needs at least four methods:
 *  - createAccumulator,
 *  - accumulateLeft,
 *  - accumulateRight, and
 *  - emitValue.
 *
 * <p>There are a few other methods that can be optional to have:
 *  - retractLeft,
 *  - retractRight,
 *  - merge
 *
 * <p>All these methods muse be declared publicly, not static and named exactly as the names
 * mentioned above. The methods createAccumulator and emitValue are defined in the
 * {@link CoTableValuedAggregateFunction} functions, while other methods are explained below.
 *
 *
 * {@code
 * Processes the input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
 * requires one accumulateLeft() and one accumulateRight() method.
 *
 * param accumulator the accumulator which contains the current aggregated results
 * param [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulateLeft(ACC accumulator, [user defined inputs])
 * public void accumulateRight(ACC accumulator, [user defined inputs])
 *}
 *
 *
 * {@code
 * Retracts the input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments.
 *
 * param accumulator           the accumulator which contains the current aggregated results
 * param [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retractLeft(ACC accumulator, [user defined inputs])
 * public void retractRight(ACC accumulator, [user defined inputs])
 * }
 *
 *
 * {@code
 * Merges a group of accumulator instances into one accumulator instance. This function must be
 * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
 *
 * param accumulator  the accumulator which will keep the merged aggregate results. It should
 *                     be noted that the accumulator may contain the previous aggregated
 *                     results. Therefore user should not replace or clean this instance in the
 *                     custom merge method.
 * param its          an {@link Iterable} pointed to a group of accumulators that will be
 *                     merged.
 *
 * public void merge(ACC accumulator, Iterable<ACC> its)
 * }
 *
 *
 * @param <T>   the type of the aggregation result
 * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
 *             aggregated values which are needed to compute an aggregation result.
 *             AggregateFunction represents its state using accumulator, thereby the state of the
 *             AggregateFunction must be put into the accumulator.
 */
public abstract class CoTableValuedAggregateFunction<T, ACC>
	extends UserDefinedAggregateFunction<ACC> {

	/**
	 * Called every time when an co-table-valued aggregation result should be materialized.
	 * The returned value could be either an early and incomplete result
	 * (periodically emitted as data arrive) or the final result of the co-table-valued aggregation.
	 *
	 * @param accumulator the accumulator which contains the current co-valued aggregated results
	 * @param out the collector used to emit row
	 */
	public abstract void emitValue(ACC accumulator, Collector<T> out);

}
