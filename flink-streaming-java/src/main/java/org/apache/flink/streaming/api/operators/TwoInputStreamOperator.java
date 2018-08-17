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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Interface for stream operators with two inputs. Use
 * {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator} as a base class if
 * you want to implement a custom operator.
 *
 * @param <IN1> The input type of the operator
 * @param <IN2> The input type of the operator
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface TwoInputStreamOperator<IN1, IN2, OUT> extends StreamOperator<OUT> {

	/**
	 * Returns the selection that this two-input operator wants to read firstly.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	default TwoInputSelection firstInputSelection() {
		return TwoInputSelection.ANY;
	}

	/**
	 * Processes one record that is from the first input of this two-input operator and returns
	 * the next input selection. This method is guaranteed to not be called concurrently with
	 * other methods of the operator.
	 *
	 * <p>You have to override this method when implementing a {@code TwoInputStreamOperator}, this is a
	 * {@code default} method for backward compatibility with the old-style method only.
	 */
	default TwoInputSelection processRecord1(StreamRecord<IN1> element) throws Exception {
		processElement1(element);

		return TwoInputSelection.ANY;
	}

	/**
	 * Processes one record that is from the second input of this two-input operator and returns
	 * the next input selection. This method is guaranteed to not be called concurrently with
	 * other methods of the operator.
	 *
	 * <p>You have to override this method when implementing a {@code TwoInputStreamOperator}, this is a
	 * {@code default} method for backward compatibility with the old-style method only.
	 */
	default TwoInputSelection processRecord2(StreamRecord<IN2> element) throws Exception {
		processElement2(element);

		return TwoInputSelection.ANY;
	}

	/**
	 * Processes one element that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @deprecated This will be removed in a future version.
	 *             Please use {@link #processRecord1(StreamRecord)} instead.
	 */
	@Deprecated
	default void processElement1(StreamRecord<IN1> element) throws Exception {}

	/**
	 * Processes one element that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @deprecated This will be removed in a future version.
	 *             Please use {@link #processRecord2(StreamRecord)} instead.
	 */
	@Deprecated
	default void processElement2(StreamRecord<IN2> element) throws Exception {}

	/**
	 * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark1(Watermark mark) throws Exception;

	/**
	 * Processes a {@link Watermark} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark2(Watermark mark) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception;

	/**
	 * Processes a {@link LatencyMarker} that arrived on the second input of this two-input operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
	 */
	void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception;

	/**
	 * It is notified that no more element ({@link StreamRecord}, {@link Watermark} and {@link LatencyMarker})
	 * will arrive on the first input of this two-input operator. This method is guaranteed to not
	 * be called concurrently with other methods of the operator.
	 */
	default void endInput1() throws Exception {}

	/**
	 * It is notified that no more element ({@link StreamRecord}, {@link Watermark} and {@link LatencyMarker})
	 * will arrive on the second input of this two-input operator. This method is guaranteed to not
	 * be called concurrently with other methods of the operator.
	 */
	default void endInput2() throws Exception {}
}
