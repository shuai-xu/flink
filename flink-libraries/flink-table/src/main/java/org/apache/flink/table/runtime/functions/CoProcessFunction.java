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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * A function that processes elements of two streams and produces a single output one.
 *
 * <p>The function will be called for every element in the input streams and can produce
 * zero or more output elements. Contrary to the {@link CoFlatMapFunction}, this function can also
 * query the time (both event and processing) and set timers, through the provided {@link Context}.
 * When reacting to the firing of set timers the function can emit yet more elements.
 *
 * <p>An example use-case for connected streams would be the application of a set of rules that change
 * over time ({@code stream A}) to the elements contained in another stream (stream {@code B}). The rules
 * contained in {@code stream A} can be stored in the state and wait for new elements to arrive on
 * {@code stream B}. Upon reception of a new element on {@code stream B}, the function can now apply the
 * previously stored rules to the element and directly emit a result, and/or register a timer that
 * will trigger an action in the future.
 *
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
public abstract class CoProcessFunction<IN1, IN2, OUT> extends ProcessFunctionBase<OUT> {

	private static final long serialVersionUID = 1L;

	public void endInput1(Collector<OUT> out) throws Exception {}

	public void endInput2(Collector<OUT> out) throws Exception {}

	/**
	 * This method is called for each element in the first of the connected streams.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param value The stream element
	 * @param ctx A {@link Context} that allows querying the timestamp of the element,
	 *            querying the {@link TimeDomain} of the firing timer and getting a
	 *            {@link TimerService} for registering timers and querying the time.
	 *            The context is only valid during the invocation of this method, do not store it.
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	public abstract void processElement1(IN1 value, ProcessFunctionBase.Context ctx, Collector<OUT> out) throws Exception;

	/**
	 * This method is called for each element in the second of the connected streams.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param value The stream element
	 * @param ctx A {@link Context} that allows querying the timestamp of the element,
	 *            querying the {@link TimeDomain} of the firing timer and getting a
	 *            {@link TimerService} for registering timers and querying the time.
	 *            The context is only valid during the invocation of this method, do not store it.
	 * @param out The collector to emit resulting elements to
	 * @throws Exception The function may throw exceptions which cause the streaming program
	 *                   to fail and go into recovery.
	 */
	public abstract void processElement2(IN2 value, ProcessFunctionBase.Context ctx, Collector<OUT> out) throws Exception;
}
