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

package org.apache.flink.test.shuffle;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.network.yarn.YarnShuffleService;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.reader.MutableRecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleService;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.io.network.partition.external.PersistentFileType;
import org.apache.flink.runtime.io.network.partition.external.YarnLocalResultPartitionResolver;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * IT case for Yarn Shuffle Service.
 */
@RunWith(Parameterized.class)
public class YarnShuffleServiceITCase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(YarnShuffleServiceITCase.class);

	private static final String USER = "blinkUser";

	private static final String APP_ID = "appid_845a3c45-e68a-4120-b61a-4617b96f4381";

	private static final int NUM_THREAD = 3;

	private static final int NUM_PRODUCERS = 3;

	private static final int NUM_CONSUMES = 3;

	private static final String NUM_RECORDS_KEY = "num_records";

	private static final String RECORD_LENGTH_KEY = "record_length";

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	private static MockYarnShuffleService mockYarnShuffleService = null;

	private static Stack<File> filesOrDirsToClear = new Stack<>();

	/** Parameterized variable on file type to use. */
	private final PersistentFileType fileType;

	/** Parameterized variable of the number of records produced by each producer. */
	private final int numRecords;

	/** Parameterized variable of the length of each record produced by each producer. */
	private final int recordLength;

	/** Parameterized variable of the length of maximum concurrent requests. */
	private final int maxConcurrentRequests;

	/** Parameterized variable of whether enable async merging or not. */
	private final boolean enableAsyncMerging;

	/** Parameterized variable of whether merge to one file or not. */
	private final boolean mergeToOneFile;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			/** Normal cases */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 4, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 4, true, true},

			/** Empty shuffle data */
			{PersistentFileType.HASH_PARTITION_FILE, 0, 128, 4, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 0, 128, 4, true, true},

			/** Limited concurrent requests */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 128, 2, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 128, 2, true, true},

			/** Different record size */
			{PersistentFileType.HASH_PARTITION_FILE, 1024, 4, 2, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, false, true},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, false},
			{PersistentFileType.MERGED_PARTITION_FILE, 1024, 4, 2, true, true},
		});
	}

	public YarnShuffleServiceITCase(PersistentFileType fileType,
									int numRecords,
									int recordLength,
									int maxConcurrentRequests,
									boolean enableAsyncMerging,
									boolean mergeToOneFile) {
		this.fileType = fileType;
		this.numRecords = numRecords;
		this.recordLength = recordLength;
		this.maxConcurrentRequests = maxConcurrentRequests;
		this.enableAsyncMerging = enableAsyncMerging;
		this.mergeToOneFile = mergeToOneFile;
	}

	@BeforeClass
	public static void before() throws Exception {
		// 1. Create a mock container-executor script used for file clearing.
		String yarnHomeEnvVar = System.getenv(ApplicationConstants.Environment.HADOOP_YARN_HOME.key());
		File hadoopBin = new File(yarnHomeEnvVar, "bin");
		File fakeContainerExecutor = new File(hadoopBin, "container-executor");

		if (!fakeContainerExecutor.exists()) {
			List<File> dirsToCreate = new ArrayList<>();

			File currentFile = hadoopBin;
			while (currentFile != null && !currentFile.exists()) {
				dirsToCreate.add(currentFile);
				currentFile = currentFile.getParentFile();
			}

			for (int i = dirsToCreate.size() - 1; i >= 0; --i) {
				boolean success = dirsToCreate.get(i).mkdir();
				if (!success) {
					throw new IOException("Failed to create the mock container-executor since the dir " + dirsToCreate + " fails to create.");
				}

				dirsToCreate.get(i).deleteOnExit();
				filesOrDirsToClear.push(dirsToCreate.get(i));
			}

			boolean success = fakeContainerExecutor.createNewFile();
			if (!success) {
				throw new IOException("Failed to create the mock container-executor file");
			}
			filesOrDirsToClear.push(fakeContainerExecutor);
		}

		// 2. Start the shuffle service.
		Throwable failCause = null;

		// Retry to avoid shuffle port conflicts.
		for (int i = 0; i < 10; ++i) {
			try {
				int port = findAvailableServerPort();
				mockYarnShuffleService = new MockYarnShuffleService(
					TEMP_FOLDER.getRoot().getAbsolutePath(),
					USER,
					APP_ID,
					port,
					NUM_THREAD);

				mockYarnShuffleService.start();
				break;
			} catch (Exception e) {
				failCause = e;

				if (mockYarnShuffleService != null) {
					mockYarnShuffleService.stop();
					mockYarnShuffleService = null;
				}
			}
		}

		assertNotNull("Fail to start yarn shuffle service, and the last retry exception " +
			"is " + ExceptionUtils.stringifyException(failCause), mockYarnShuffleService);
	}

	@AfterClass
	public static void after() {
		// 1. Create all the files or dirs.
		while (!filesOrDirsToClear.empty()) {
			filesOrDirsToClear.pop().delete();
		}

		// 2. Shutdown the shuffle service.
		if (mockYarnShuffleService != null) {
			mockYarnShuffleService.stop();
		}
	}

	@Test
	public void testShuffleService() throws Exception {
		Configuration configuration = prepareConfiguration(fileType);
		executeShuffleTest(configuration);
	}

	public Configuration prepareConfiguration(PersistentFileType externalFileType) {
		Configuration configuration = new Configuration();

		configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_ENABLE_ASYNC_MERGE, enableAsyncMerging);
		configuration.setBoolean(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_TO_ONE_FILE, mergeToOneFile);
		if (PersistentFileType.HASH_PARTITION_FILE == externalFileType) {
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 16);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 16);
		} else if (PersistentFileType.MERGED_PARTITION_FILE == externalFileType) {
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, 1);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_HASH_MAX_SUBPARTITIONS, 2);
			configuration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MERGE_FACTOR, 2);
		} else {
			throw new IllegalArgumentException("Invalid configuration for ExternalFileType");
		}

		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 1L << 20);
		configuration.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 1L << 20);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL, 4);

		configuration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, BlockingShuffleType.YARN.toString());
		configuration.setInteger(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), mockYarnShuffleService.getPort());

		configuration.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS, maxConcurrentRequests);

		// Add local shuffle dirs
		configuration.setString(TaskManagerOptions.TASK_MANAGER_OUTPUT_LOCAL_OUTPUT_DIRS,
			TEMP_FOLDER.getRoot().getAbsolutePath() + "/" + YarnLocalResultPartitionResolver.generateRelativeLocalAppDir(USER, APP_ID));

		// Use random port to avoid port conflict
		configuration.setInteger(RestOptions.PORT, 0);

		return configuration;
	}

	private void executeShuffleTest(Configuration configuration) throws Exception {
		JobGraph jobGraph = createJobGraph();

		int numTaskManager = 0;
		for (JobVertex jobVertex : jobGraph.getVertices()) {
			numTaskManager += jobVertex.getParallelism();
		}

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(numTaskManager)
			.setNumSlotsPerTaskManager(1)
			.build();

		try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
			miniCluster.start();

			MiniClusterClient miniClusterClient = new MiniClusterClient(configuration, miniCluster);
			CompletableFuture<JobSubmissionResult> submissionFuture = miniClusterClient.submitJob(jobGraph);

			// wait for the submission to succeed
			JobSubmissionResult jobSubmissionResult = submissionFuture.get();
			CompletableFuture<JobResult> resultFuture = miniClusterClient.requestJobResult(jobSubmissionResult.getJobID());

			JobResult jobResult = resultFuture.get();
			assertThat(jobResult.getSerializedThrowable().toString(), jobResult.getSerializedThrowable().isPresent(), is(false));

			// Try best to release the direct memory by GC the DirectByteBuffer objects.
			System.gc();
		}
	}

	private JobGraph createJobGraph() throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED);
		env.getConfig().registerKryoType(TestRecord.class);

		JobGraph jobGraph = new JobGraph("Yarn Shuffle Service Test");

		jobGraph.setExecutionConfig(env.getConfig());
		jobGraph.setAllowQueuedScheduling(true);

		JobVertex producer = new JobVertex("Yarn Shuffle Service Test Producer");
		jobGraph.addVertex(producer);
		producer.setSlotSharingGroup(new SlotSharingGroup());
		producer.setInvokableClass(TestProducer.class);
		producer.setParallelism(NUM_PRODUCERS);

		JobVertex consumer = new JobVertex("Yarn Shuffle Service Test Consumer");
		jobGraph.addVertex(consumer);
		consumer.setSlotSharingGroup(new SlotSharingGroup());
		consumer.setInvokableClass(TestConsumer.class);
		consumer.setParallelism(NUM_CONSUMES);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		jobGraph.getJobConfiguration().setInteger(NUM_RECORDS_KEY, numRecords);
		jobGraph.getJobConfiguration().setInteger(RECORD_LENGTH_KEY, recordLength);

		return jobGraph;
	}

	private static int findAvailableServerPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/**
	 * A mock shuffle service server.
	 */
	private static class MockYarnShuffleService {
		private final org.apache.hadoop.conf.Configuration
			hadoopConf = new org.apache.hadoop.conf.Configuration();

		private final String user;

		private final String appId;

		private final int port;

		private ExternalBlockShuffleService shuffleService = null;

		public MockYarnShuffleService(String externalDir, String user, String appId, int port, int threadNum) {
			this.user = user;
			this.appId = appId;
			this.port = port;

			hadoopConf.setStrings(ExternalBlockShuffleServiceOptions.LOCAL_DIRS.key(), "[test]" + externalDir);
			hadoopConf.setInt(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), port);
			hadoopConf.setStrings(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE.key(), "test: " + threadNum);
			hadoopConf.setInt(ExternalBlockShuffleServiceOptions.MIN_BUFFER_NUMBER.key(), 20);
		}

		public int getPort() {
			return port;
		}

		public void start() throws Exception {
			// Postpone the creation of shuffle service till start to avoid startings all the thread pool in advance.
			shuffleService = new ExternalBlockShuffleService(YarnShuffleService.fromHadoopConfiguration(hadoopConf));
			shuffleService.start();
			// shuffle service need to use this message to map appId to user
			shuffleService.initializeApplication(user, appId);
		}

		public void stop() {
			if (shuffleService != null) {
				shuffleService.stopApplication(appId);
				shuffleService.stop();
				shuffleService = null;
			}
		}
	}

	/**
	 * Data producer for IT case.
	 */
	public static class TestProducer extends AbstractInvokable {
		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestProducer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			ResultPartitionWriter rpw = getEnvironment().getWriter(0);
			rpw.setParentTask(this);
			rpw.setTypeSerializer(TypeExtractor.createTypeInfo(TestRecord.class).createSerializer(getEnvironment().getExecutionConfig()));
			RecordWriter<TestRecord> writer = new RecordWriter<>(rpw);

			int numRecords = getEnvironment().getJobConfiguration().getInteger(NUM_RECORDS_KEY, -1);
			assertTrue("Number of records not set", numRecords >= 0);

			int recordLength = getEnvironment().getJobConfiguration().getInteger(RECORD_LENGTH_KEY, -1);
			assertTrue("Record length not set", recordLength > 0);

			try {
				for (int i = 0; i < numRecords; ++i) {
					writer.broadcastEmit(new TestRecord(i, recordLength));
				}
			} finally {
				writer.flushAll();
			}
		}
	}

	/**
	 * Data consumer for IT case.
	 */
	public static class TestConsumer extends AbstractInvokable {
		/**
		 * Create an Invokable task and set its environment.
		 *
		 * @param environment The environment assigned to this invokable.
		 */
		public TestConsumer(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			int numRecords = getEnvironment().getJobConfiguration().getInteger(NUM_RECORDS_KEY, -1);
			assertTrue("Number of records not set", numRecords >= 0);

			int recordLength = getEnvironment().getJobConfiguration().getInteger(RECORD_LENGTH_KEY, -1);
			assertTrue("Record length not set", recordLength > 0);

			Map<TestRecord, Integer> recordsCount = new HashMap<>();

			MutableRecordReader<DeserializationDelegate<TestRecord>> reader = new MutableRecordReader<>(
				getEnvironment().getInputGate(0),
				getEnvironment().getTaskManagerInfo().getTmpDirectories());

			try {
				DeserializationDelegate<TestRecord> deserializationDelegate = new NonReusingDeserializationDelegate<>(
					TypeExtractor.createTypeInfo(TestRecord.class).createSerializer(getEnvironment().getExecutionConfig()));

				while (reader.next(deserializationDelegate)) {
					TestRecord record = deserializationDelegate.getInstance();
					recordsCount.compute(record, (key, oldCount) -> oldCount == null ? 1 : oldCount + 1);
				}

				for (int i = 0; i < numRecords; ++i) {
					TestRecord record = new TestRecord(i, recordLength);
					int count = recordsCount.getOrDefault(record, 0);
					assertEquals(NUM_PRODUCERS, count);
				}
			} finally {
				reader.clearBuffers();
			}
		}
	}

	/**
	 * The records sent by the producers.
	 */
	public static class TestRecord {
		private int index;
		private int recordLength;
		private byte[] buf;

		public TestRecord() {
			// For deserialization.
		}

		public TestRecord(int index, int recordLength) {
			this.index = index;
			this.recordLength = recordLength;

			this.buf = new byte[recordLength];

			// Random with the same seed will always generate the same random number sequence.
			Random random = new Random(index);
			for (int i = 0; i < recordLength; ++i) {
				buf[i] = (byte) (random.nextInt() % 128);
			}
		}

		public int getIndex() {
			return index;
		}

		public byte[] getByte() {
			return buf;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestRecord that = (TestRecord) o;
			return index == that.index &&
				Arrays.equals(buf, that.buf);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(index);
			result = 31 * result + Arrays.hashCode(buf);
			return result;
		}

		@Override
		public String toString() {
			StringBuilder ret = new StringBuilder();

			ret.append(index).append(": ");
			for (int i = 0; i < recordLength; ++i) {
				ret.append(buf[i]).append(" ");
			}
			return ret.toString();
		}
	}
}
