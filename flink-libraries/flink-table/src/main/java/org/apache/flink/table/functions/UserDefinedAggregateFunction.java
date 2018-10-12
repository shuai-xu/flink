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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;

/**
 * Base class for User-Defined Aggregates.
 *
 * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
 *             aggregated values which are needed to compute an aggregation result.
 *             AggregateFunction represents its state using accumulator, thereby the state of the
 *             AggregateFunction must be put into the accumulator.
 */
public abstract class UserDefinedAggregateFunction<ACC> extends UserDefinedFunction {

	/**
	 * Creates and init the Accumulator for this {@link UserDefinedAggregateFunction}.
	 *
	 * @return the accumulator with the initial value
	 */
	public abstract ACC createAccumulator();

	/**
	 * Returns the DataType of the AggregateFunction's result.
	 *
	 * @return The DataType of the AggregateFunction's result or null if the result type
	 *         should be automatically inferred.
	 */
	public DataType getResultType() {
		return null;
	}

	/**
	 * Returns the DataType of the AggregateFunction's accumulator.
	 *
	 * @return The DataType of the AggregateFunction's accumulator or null if the
	 *         accumulator type should be automatically inferred.
	 */
	public DataType getAccumulatorType() {
		return null;
	}

	public DataType[] getUserDefinedInputTypes(Class[] signature) {
		DataType[] types = new DataType[signature.length];
		try {
			for (int i = 0; i < signature.length; i++) {
				types[i] = DataTypes.of(TypeExtractor.getForClass(signature[i]));
			}
		} catch (InvalidTypesException e) {
			throw new ValidationException(
				"Parameter types of aggregate function '" +
				getClass().getCanonicalName() + "' cannot be automatically determined. " +
				"Please provide data type manually.");
		}
		return types;
	}

	/**
	 * Returns true if this AggregateFunction can only be applied in an OVER window.
	 *
	 * @return true if the AggregateFunction requires an OVER window, false otherwise.
	 */
	public boolean requiresOver() {
		return false;
	}

}
