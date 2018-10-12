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

import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;

/**
 * A function that processes elements of a stream.
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)}
 * is invoked. This can produce zero or more elements as output. Implementations can also
 * query the time and set timers through the provided {@link Context}. For firing timers
 * {@link #onTimer(long, OnTimerContext, Collector)} will be invoked. This can again produce
 * zero or more elements as output and register further timers.
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code ProcessFunction} is applied on a {@code KeyedStream}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
public abstract class ProcessFunction<I, O> extends ProcessFunctionBase<O> {

	private static final long serialVersionUID = 1L;

	public void endInput(Collector<O> out) throws Exception {}

	/**
	 * Process one element from the input stream.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param input The input value.
	 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
	 *            a {@link TimerService} for registering timers and querying the time. The
	 *            context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	public abstract void processElement(I input, ProcessFunctionBase.Context ctx, Collector<O> out) throws Exception;
}
