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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.HashPartitioner;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.GroupRangePartitioner;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.IncrementalStatePartitionSnapshot;
import org.apache.flink.runtime.state.InternalState;
import org.apache.flink.runtime.state.InternalStateDescriptor;
import org.apache.flink.runtime.state.InternalStateDescriptorBuilder;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StatePartitionSnapshot;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.util.BlockerCheckpointStreamFactory;
import org.apache.flink.runtime.util.BlockingCheckpointOutputStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.ResourceGuard;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for {@link RocksDBInternalStateBackend}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest(ResourceGuard.class)
public class RocksDBInternalStateBackendTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private DBOptions dbOptions;

	private ColumnFamilyOptions columnOptions;

	private RocksDBInternalStateBackend backend;

	private OneShotLatch blocker;

	private OneShotLatch waiter;

	private BlockerCheckpointStreamFactory testStreamFactory;

	private ResourceGuard rocksDBResourceGuard;

	private List<AutoCloseable> autoCloseables;

	private final HashPartitioner partitioner = HashPartitioner.INSTANCE;

	private final int maxParallism = 10;

	private InternalStateDescriptor stateDescriptor1 =
		new InternalStateDescriptorBuilder("state1")
			.addKeyColumn("key", IntSerializer.INSTANCE)
			.addValueColumn("value", FloatSerializer.INSTANCE).getDescriptor();

	private InternalState state1;

	private InternalStateDescriptor stateDescriptor2 =
		new InternalStateDescriptorBuilder("state2")
			.addKeyColumn("key", IntSerializer.INSTANCE)
			.addKeyColumn("mk", IntSerializer.INSTANCE)
			.addValueColumn("mv", FloatSerializer.INSTANCE).getDescriptor();

	private InternalState state2;

	@Parameterized.Parameters(name = "Incremental checkpointing: {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean enableIncrementalCheckpointing;

	@Before
	public void setupOptions() {
		dbOptions = spy(new DBOptions().setCreateIfMissing(true));
		columnOptions = spy(new ColumnFamilyOptions());
	}

	@After
	public void cleanup() {
		if (backend != null) {
			backend.closeImpl();
			backend = null;
		}

		if (dbOptions != null) {
			dbOptions.close();
		}
		if (columnOptions != null) {
			columnOptions.close();
		}
	}

	private void setupRocksDBStateBackend() throws Exception {
		blocker = new OneShotLatch();
		waiter = new OneShotLatch();
		testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		testStreamFactory.setBlockerLatch(blocker);
		testStreamFactory.setWaiterLatch(waiter);
		testStreamFactory.setAfterNumberInvocations(10);

		backend =  new RocksDBInternalStateBackend(
			ClassLoader.getSystemClassLoader(),
			tempFolder.newFolder(),
			dbOptions,
			columnOptions,
			maxParallism,
			getGroupsForSubtask(maxParallism, 1, 0),
			true,
			mock(LocalRecoveryConfig.class),
			null);
		backend.restore(null);

		state1 = backend.getInternalState(stateDescriptor1);
		state2 = backend.getInternalState(stateDescriptor2);

		backend.dbInstance = spy(backend.dbInstance);

		// spy on final 'rocksDBResourceGuard' field
		final Field resourceGuardField = RocksDBInternalStateBackend.class.getDeclaredField("rocksDBResourceGuard");
		resourceGuardField.setAccessible(true);
		rocksDBResourceGuard = spy(backend.rocksDBResourceGuard);
		resourceGuardField.set(backend, rocksDBResourceGuard);

		for (int i = 0; i < 100; i++) {
			Row internalKey1 = Row.of(i);
			Row internalKey2 = Row.of(i, ThreadLocalRandom.current().nextInt());
			state1.setCurrentGroup(partitioner.partition(internalKey1, maxParallism));
			state1.put(internalKey1, Row.of(ThreadLocalRandom.current().nextFloat()));
			state2.setCurrentGroup(partitioner.partition(internalKey2, maxParallism));
			state2.put(internalKey2, Row.of(ThreadLocalRandom.current().nextFloat()));
		}

		autoCloseables = new ArrayList<>();

		autoCloseables.add(dbOptions);
		autoCloseables.add(columnOptions);
		autoCloseables.add(backend.dbInstance);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				RocksIterator rocksIterator = spy((RocksIterator) invocation.callRealMethod());
				autoCloseables.add(rocksIterator);
				return rocksIterator;
			}
		}).when(backend.dbInstance).iterator();
	}

	@Test
	public void testCorrectMergeOperatorSet() throws IOException {
		ColumnFamilyOptions columnFamilyOptions = spy(columnOptions);
		RocksDBInternalStateBackend test = null;
		try {
			test = new RocksDBInternalStateBackend(
				ClassLoader.getSystemClassLoader(),
				tempFolder.newFolder(),
				dbOptions,
				columnFamilyOptions,
				10,
				getGroupsForSubtask(10, 1, 0),
				true,
				mock(LocalRecoveryConfig.class),
				null);

			verify(columnFamilyOptions, Mockito.times(1))
				.setMergeOperatorName(RocksDBInternalStateBackend.MERGE_OPERATOR_NAME);
		} finally {
			if (test != null) {
				test.closeImpl();
			}
			columnFamilyOptions.close();
		}
	}

	@Test
	public void testReleasingRocksObjectsAfterBackendClosed() throws Exception {
		setupRocksDBStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

			RocksDBInstance spyDB = backend.getDbInstance();

			verify(spyDB, times(1)).snapshot(any(String.class));

			//Ensure every RocksObjects not closed yet
			for (AutoCloseable rocksCloseable : autoCloseables) {
				verify(rocksCloseable, times(0)).close();
			}

			snapshot.cancel(true);

			this.backend.closeImpl();

			verify(spyDB, times(1)).close();
			assertNull(backend.dbInstance);

			//Ensure every RocksObjects was closed exactly once
			for (AutoCloseable rocksCloseable : autoCloseables) {
				verify(rocksCloseable, times(1)).close();
			}

		} finally {
			backend.closeImpl();
			backend = null;
		}
	}

	@Test
	public void testDismissingSnapshot() throws Exception {
		setupRocksDBStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			verify(rocksDBResourceGuard, times(1)).acquireResource();
			assertEquals(1, rocksDBResourceGuard.getLeaseCount());
			assertEquals(0, testStreamFactory.getAllCreatedStreams().size());
			snapshot.cancel(true);
			assertEquals(0, rocksDBResourceGuard.getLeaseCount());

		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testDismissingSnapshotNotRunnable() throws Exception {
		setupRocksDBStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			assertEquals(1, rocksDBResourceGuard.getLeaseCount());
			snapshot.cancel(true);
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {

			}
			asyncSnapshotThread.join();
			verify(rocksDBResourceGuard, times(1)).acquireResource();
			assertEquals(0, testStreamFactory.getAllCreatedStreams().size());
			assertEquals(0, rocksDBResourceGuard.getLeaseCount());

		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testCompletingSnapshot() throws Exception {
		setupRocksDBStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshotFuture =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshotFuture);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			blocker.trigger(); // allow checkpointing to start writing
			waiter.await(); // wait for snapshot stream writing to run

			SnapshotResult<StatePartitionSnapshot> snapshotResult = snapshotFuture.get();
			StatePartitionSnapshot snapshot = snapshotResult.getJobManagerOwnedSnapshot();
			assertNotNull(snapshot);
			assertTrue(snapshot.getStateSize() > 0);
			assertEquals(new GroupRange(0, 10), snapshot.getGroups());

			// meta file, at least one SST file, MANIFEST file, CURRENT file and OPTIONS file.
			assertTrue(testStreamFactory.getAllCreatedStreams().size() >= 5);
			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			asyncSnapshotThread.join();
		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testCancelRunningSnapshot() throws Exception {
		setupRocksDBStateBackend();
		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			waiter.await(); // wait for snapshot to run
			waiter.reset();
			runStateUpdates();
			snapshot.cancel(true);
			blocker.trigger(); // allow checkpointing to start writing

			assertFalse(testStreamFactory.getAllCreatedStreams().isEmpty());
			for (BlockingCheckpointOutputStream stream : testStreamFactory.getAllCreatedStreams()) {
				assertTrue(stream.isClosed());
			}

			waiter.await(); // wait for snapshot stream writing to run
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {
			}

			asyncSnapshotThread.join();
		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testCloseImplDeletesAllDirectories() throws Exception {
		setupRocksDBStateBackend();

		File dbPath = backend.getInstanceBasePath();
		Collection<File> allFilesInDbDir =
			FileUtils.listFilesAndDirs(dbPath, new AcceptAllFilter(), new AcceptAllFilter());
		try {

			// more than just the root directory
			assertTrue(allFilesInDbDir.size() > 1);
		} finally {
			backend.closeImpl();
		}
		assertFalse(dbPath.exists());
	}

	@Test
	public void testSharedIncrementalStateDeRegistration() throws Exception {
		if (enableIncrementalCheckpointing) {
			setupRocksDBStateBackend();

			CheckpointStorage checkpointStorage = new FsStateBackend(tempFolder.newFolder().toURI().toString()).createCheckpointStorage(new JobID());
			try {
				Queue<IncrementalStatePartitionSnapshot> previousStateHandles = new LinkedList<>();
				SharedStateRegistry sharedStateRegistry = spy(new SharedStateRegistry());

				for (int checkpointId = 0; checkpointId < 3; ++checkpointId) {

					CheckpointStorageLocation checkpointStorageLocation = checkpointStorage.initializeLocationForCheckpoint(checkpointId);

					reset(sharedStateRegistry);

					runStateUpdates();

					RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshotFuture =
						backend.snapshot(checkpointId, checkpointId, checkpointStorageLocation, CheckpointOptions.forCheckpointWithDefaultLocation());

					SnapshotResult<StatePartitionSnapshot> snapshotResult = FutureUtil.runIfNotDoneAndGet(snapshotFuture);
					StatePartitionSnapshot snapshot = snapshotResult.getJobManagerOwnedSnapshot();

					IncrementalStatePartitionSnapshot stateHandle =
						(IncrementalStatePartitionSnapshot) snapshot;

					Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedState =
						new HashMap<>(stateHandle.getSharedState());

					stateHandle.registerSharedStates(sharedStateRegistry);

					for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> e : sharedState.entrySet()) {
						verify(sharedStateRegistry).registerReference(
							stateHandle.createSharedStateRegistryKeyFromUniqueId(e.getValue().f0, e.getKey()), e.getValue().f1);
					}

					previousStateHandles.add(stateHandle);
					backend.notifyCheckpointComplete(checkpointId);

					//-----------------------------------------------------------------

					if (previousStateHandles.size() > 1) {
						checkRemove(previousStateHandles.remove(), sharedStateRegistry);
					}
				}

				while (!previousStateHandles.isEmpty()) {

					reset(sharedStateRegistry);

					checkRemove(previousStateHandles.remove(), sharedStateRegistry);
				}
			} finally {
				backend.closeImpl();
			}
		}
	}

	private GroupSet getGroupsForSubtask(int maxParallelism, int parallelism, int subtaskIndex) {
		GroupRange groups = new GroupRange(0, maxParallelism);
		return GroupRangePartitioner.getPartitionRange(groups, parallelism, subtaskIndex);
	}

	private void runStateUpdates() throws Exception{
		for (int i = 50; i < 150; ++i) {
			if (i % 10 == 0) {
				Thread.sleep(1);
			}
			state1.put(Row.of(i), Row.of(ThreadLocalRandom.current().nextFloat()));
			state2.put(Row.of(i, ThreadLocalRandom.current().nextInt()), Row.of(ThreadLocalRandom.current().nextFloat()));
		}
	}

	private void checkRemove(IncrementalStatePartitionSnapshot remove, SharedStateRegistry registry) throws Exception {

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : remove.getSharedState().entrySet()) {
			verify(registry, times(0)).unregisterReference(
				remove.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey()));
		}

		remove.discardState();

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : remove.getSharedState().entrySet()) {
			verify(registry).unregisterReference(
				remove.createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey()));
		}
	}

	private static class AcceptAllFilter implements IOFileFilter {
		@Override
		public boolean accept(File file) {
			return true;
		}

		@Override
		public boolean accept(File file, String s) {
			return true;
		}
	}
}
