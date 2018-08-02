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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * A test Program with the following job graph:
 * <pre>
 *  A     B
 *   \   /
 *   \  /
 *   \ /
 *    C
 * </pre>
 *
 * <p>Each task of B wants to output numbers from 1 to MAX_NUMBER, and C output the received numbers.
 * During this process, each task of A will commit a value equals to its subtask index, and numbers
 * match these indices will be filtered out. Pre-aggregated accumulators are used to help B to finish the filter in
 * advance, and C will execute the final filter in case the accumulators do not arrive in time.
 */
public class PreAggregatedAccumulatorProgram {
	private static final int PARALLELISM = 1;
	private static final int MAX_NUMBER = 1000;
	private static final String ACCUMULATOR_NAME = "test";

	private static final ConcurrentHashMap<String, ExecutionContext> EXECUTION_CONTEXTS = new ConcurrentHashMap<>();

	public static Map<Integer, Integer> executeJob(ExecutionMode executionMode,
													StreamExecutionEnvironment env) throws Exception {
		env.setParallelism(PARALLELISM);

		// Create a random id for this execution so that different execution will not interfere with others.
		final String contextId = RandomStringUtils.random(16);
		EXECUTION_CONTEXTS.put(contextId, new ExecutionContext(executionMode));

		DataStream<Integer> left = env.addSource(new AccumulatorProducerSourceFunction(contextId));
		DataStream<Integer> right = env.addSource(new AccumulatorConsumerSourceFunction(contextId));
		left.connect(right).flatMap(new IntegerJoinFunction(contextId));

		env.execute();

		ExecutionContext executionContext = EXECUTION_CONTEXTS.remove(contextId);
		return executionContext.getNumberReceived();
	}

	public static void assertResultConsistent(Map<Integer, Integer> numberReceived) {
		for (int i = 0; i < PARALLELISM; ++i) {
			assertFalse(numberReceived.containsKey(i));
		}

		for (int i = PARALLELISM; i < MAX_NUMBER; ++i) {
			assertEquals(new Integer(PARALLELISM), numberReceived.get(i));
		}
	}

	/**
	 * Execution mode of the test program, which indicates the execution order of the two sources.
	 */
	public enum ExecutionMode {
		CONSUMER_WAIT_QUERY_FINISH,
		PRODUCER_WAIT_CONSUMER_FINISH
	}

	/**
	 * Wrapper class for global objects used in test program, including the latch to coordinate the
	 * execution order and the map to hold the result.
	 */
	public static class ExecutionContext {
		private final ExecutionMode executionMode;
		private final CountDownLatch consumerFinishedLatch = new CountDownLatch(PARALLELISM);
		private final ConcurrentHashMap<Integer, Integer> numberReceived = new ConcurrentHashMap<>();

		public ExecutionContext(ExecutionMode executionMode) {
			this.executionMode = executionMode;
		}

		public ExecutionMode getExecutionMode() {
			return executionMode;
		}

		public CountDownLatch getConsumerFinishedLatch() {
			return consumerFinishedLatch;
		}

		public ConcurrentHashMap<Integer, Integer> getNumberReceived() {
			return numberReceived;
		}
	}

	/**
	 * The source who produce the tested accumulator.
	 */
	private static class AccumulatorProducerSourceFunction extends RichParallelSourceFunction<Integer> {
		private final String contextId;

		public AccumulatorProducerSourceFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addPreAggregatedAccumulator(ACCUMULATOR_NAME, new IntegerSetAccumulator());
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ExecutionMode executionMode = EXECUTION_CONTEXTS.get(contextId).getExecutionMode();
			CountDownLatch consumerFinished = EXECUTION_CONTEXTS.get(contextId).getConsumerFinishedLatch();

			int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

			if (subtaskIndex == 0 && executionMode == ExecutionMode.PRODUCER_WAIT_CONSUMER_FINISH) {
				consumerFinished.await();
			}

			getRuntimeContext().getPreAggregatedAccumulator(ACCUMULATOR_NAME).add(subtaskIndex);
			getRuntimeContext().commitPreAggregatedAccumulator(ACCUMULATOR_NAME);
		}

		@Override
		public void cancel() {
		}
	}

	/**
	 * The source who consumes the tested accumulator.
	 */
	private static class AccumulatorConsumerSourceFunction extends RichParallelSourceFunction<Integer> {
		private final String contextId;

		private CountDownLatch globalFilterReceived;
		private volatile Accumulator<Integer, IntegerSetAccumulator.IntegerSet> receivedAccumulator;

		public AccumulatorConsumerSourceFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			globalFilterReceived = new CountDownLatch(1);

			CompletableFuture<Accumulator<Integer, IntegerSetAccumulator.IntegerSet>> filterCompletableFuture =
				getRuntimeContext().queryPreAggregatedAccumulator(ACCUMULATOR_NAME);

			filterCompletableFuture.whenComplete((receivedAccumulator, throwable) -> {
				if (receivedAccumulator != null) {
					this.receivedAccumulator = receivedAccumulator;
					globalFilterReceived.countDown();
				}
			});
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			ExecutionMode executionMode = EXECUTION_CONTEXTS.get(contextId).getExecutionMode();
			CountDownLatch consumerFinished = EXECUTION_CONTEXTS.get(contextId).getConsumerFinishedLatch();

			if (executionMode == ExecutionMode.CONSUMER_WAIT_QUERY_FINISH) {
				//TODO: will be uncomment later
//				globalFilterReceived.await();
			}

			for (int i = 0; i < MAX_NUMBER; ++i) {
				final Accumulator<Integer, IntegerSetAccumulator.IntegerSet> currentAccumulator = receivedAccumulator;

				if (currentAccumulator == null || !currentAccumulator.getLocalValue().getIntegers().contains(i)) {
					ctx.collect(i);
				}
			}

			consumerFinished.countDown();
		}

		@Override
		public void cancel() {

		}
	}

	/**
	 * Join the result of the producer and consumer source. In this case, only the consumer source
	 * has output.
	 */
	private static class IntegerJoinFunction implements CoFlatMapFunction<Integer, Integer, Integer> {
		private final String contextId;

		public IntegerJoinFunction(String contextId) {
			this.contextId = contextId;
		}

		@Override
		public void flatMap1(Integer value, Collector<Integer> out) {
			// Do nothing.
		}

		@Override
		public void flatMap2(Integer value, Collector<Integer> out) {
			ConcurrentHashMap<Integer, Integer> numberReceived = EXECUTION_CONTEXTS.get(contextId).getNumberReceived();

			if (value >= PARALLELISM) {
				numberReceived.compute(value, (k, v) -> v == null ? 1 : v + 1);
			}
		}
	}
}
