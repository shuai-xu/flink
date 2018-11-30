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

import org.apache.flink.api.common.typeutils.base.StringValueSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.io.network.partition.external.YarnLocalResultPartitionResolver;
import org.apache.flink.types.StringValue;

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

import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.openjdk.jmh.annotations.Scope.Thread;

/**
 * JMH throughput benchmark runner.
 */
@OperationsPerInvocation(value = StreamNetworkYarnShuffleServiceThroughputBenchmarkExecutor.RECORDS_PER_INVOCATION)
public class StreamNetworkYarnShuffleServiceThroughputBenchmarkExecutor extends BenchmarkBase {

	static final int RECORDS_PER_INVOCATION = 1_000_000;

	public static void main(String[] args)
		throws RunnerException {
		Options options = new OptionsBuilder()
			.verbosity(VerboseMode.NORMAL)
			.include(".*" + StreamNetworkYarnShuffleServiceThroughputBenchmarkExecutor.class.getSimpleName() + ".*")
			.build();

		new Runner(options).run();
	}

	@Benchmark
	public void networkYarnShuffleServiceThroughput(MultiEnvironment context) throws Exception {
		context.executeBenchmark();
	}

	/**
	 * Setup for the benchmark(s).
	 */
	@State(Thread)
	public static class MultiEnvironment extends StreamNetworkYarnShuffleServiceThroughputBenchmark<StringValue> {

		@Param({"125", "10", "2"})
		public int mergeFactor = 2;

		@Param({"true", "false"})
		public boolean enableAsyncMerging = false;

		@Param({"200,true", "200,false", "1024,true"})
		public String maxFileAndMergeToOneFile = "200,true";

		@Setup
		public void setUp() throws Exception {
			int numRecordWriters = 1;
			int numRecordReceivers = 4;
			int numChannels = 1000;
			int flushTimeout = 100;
			int numBufferPerChannel = 64;
			int receiverBufferPoolSize = numBufferPerChannel * numChannels * numRecordWriters;
			Tuple2<Integer, Boolean> maxFileAndUseHashWriterTuple = parseMaxFileAndUseHashWriter(maxFileAndMergeToOneFile);
			Configuration configuration = createConfiguration(
				numRecordWriters,
				maxFileAndUseHashWriterTuple.f0,
				mergeFactor,
				numBufferPerChannel,
				enableAsyncMerging,
				maxFileAndUseHashWriterTuple.f1);
			super.setUp(
				numRecordWriters,
				numRecordReceivers,
				numChannels,
				flushTimeout,
				RECORDS_PER_INVOCATION,
				receiverBufferPoolSize,
				generateRecords(),
				new StringValue(),
				new StringValueSerializer(),
				configuration);
		}

		private static StringValue[] generateRecords() {
			// generate 1000 string of 1kB each
			StringValue[] records = new StringValue[1000];
			Random random = new Random();
			for (int i = 0; i < 1000; ++i) {
				byte[] bytes = new byte[1024];
				random.nextBytes(bytes);
				records[i] = new StringValue(new String(bytes));
			}
			return records;
		}

		private static Tuple2<Integer, Boolean> parseMaxFileAndUseHashWriter(String maxFileAndNotSpillOneFile) {
			String[] parameters = maxFileAndNotSpillOneFile.split(",");
			checkArgument(parameters.length == 2);
			int maxFile = Integer.parseInt(parameters[0]);
			boolean notSpillOneFile = Boolean.parseBoolean(parameters[1]);
			return new Tuple2<>(maxFile, notSpillOneFile);
		}

		private Configuration createConfiguration(
			int recordWriters,
			int maxFile,
			int mergeFactor,
			int numBufferPerChannel,
			boolean enableAsyncMerging,
			boolean mergeToOneFile) {
			Configuration configuration = new Configuration();
			for (int i = 0; i < recordWriters; ++i) {
				configuration.setDouble("memory.shuffle." + i, 32);
			}

			String spillPath = spillRootPath + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(user, appId);

			configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL, numBufferPerChannel);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, maxFile);
			configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE, mergeToOneFile);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, mergeFactor);
			configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE, enableAsyncMerging);
			configuration.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS, spillPath);
			configuration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, "YARN");

			return configuration;
		}

		@TearDown
		public void tearDown() throws Exception {
			super.tearDown();
		}
	}
}

