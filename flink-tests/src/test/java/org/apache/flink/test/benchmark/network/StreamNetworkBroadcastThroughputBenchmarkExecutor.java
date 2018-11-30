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

package org.apache.flink.test.benchmark.network;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongValueSerializer;
import org.apache.flink.types.LongValue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * JMH throughput benchmark runner.
 */
@OperationsPerInvocation(value = StreamNetworkBroadcastThroughputBenchmarkExecutor.RECORDS_PER_INVOCATION)
public class StreamNetworkBroadcastThroughputBenchmarkExecutor extends BenchmarkBase {

	static final int RECORDS_PER_INVOCATION = 500_000;

	public static void main(String[] args)
		throws RunnerException {
		Options options = new OptionsBuilder()
			.verbosity(VerboseMode.NORMAL)
			.include(".*" + StreamNetworkBroadcastThroughputBenchmarkExecutor.class.getSimpleName() + ".*")
			.build();

		new Runner(options).run();
	}

	@Benchmark
	public void networkBroadcastThroughput(MultiEnvironment context) throws Exception {
		context.executeBenchmark();
	}

	/**
	 * Setup for the benchmark(s).
	 */
	@State(Thread)
	public static class MultiEnvironment extends StreamNetworkThroughputBenchmark<LongValue> {

		@Param({"1", "4"})
		public int numWriters = 4;

		@Param({"1", "4"})
		public int numReceivers = 4;

		@Param({"10", "100"})
		public int numChannels = 100;

		@Setup
		public void setUp() throws Exception {
			super.setUp(
				numWriters,
				numReceivers,
				numChannels,
				100,
				RECORDS_PER_INVOCATION,
				new LongValue[] {new LongValue(0)},
				new LongValue(),
				new LongValueSerializer());
		}

		@Override
		public void setUp(
			int recordWriters,
			int recordReceivers,
			int channels,
			int flushTimeout,
			long numRecordToSend,
			boolean localMode,
			int senderBufferPoolSize,
			int receiverBufferPoolSize,
			LongValue[] recordsSet,
			LongValue value,
			TypeSerializer<LongValue> serializer) throws Exception {
			setUp(
				recordWriters,
				recordReceivers,
				channels,
				flushTimeout,
				numRecordToSend,
				true,
				localMode,
				senderBufferPoolSize,
				receiverBufferPoolSize,
				recordsSet,
				value,
				serializer);
		}

		@TearDown
		public void tearDown() throws Exception {
			super.tearDown();
		}
	}
}

