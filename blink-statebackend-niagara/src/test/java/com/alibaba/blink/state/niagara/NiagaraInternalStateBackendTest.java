/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

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

import com.alibaba.niagara.AbstractImmutableNativeReference;
import com.alibaba.niagara.NiagaraIterator;
import com.alibaba.niagara.ReadOptions;
import com.alibaba.niagara.TabletOptions;
import com.alibaba.niagara.WriteOptions;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for {@link NiagaraInternalStateBackend}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest(ResourceGuard.class)
public class NiagaraInternalStateBackendTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Parameterized.Parameters(name = "Incremental checkpointing: {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean enableIncrementalCheckpointing;

	private List<AbstractImmutableNativeReference> niagaraObjects;

	private NiagaraInternalStateBackend backend;

	private OneShotLatch blocker;

	private OneShotLatch waiter;

	private BlockerCheckpointStreamFactory testStreamFactory;

	private ResourceGuard niagaraResourceGuard;

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

	@BeforeClass
	public static void platformCheck() throws IOException {
		// Check OS & Kernel version before run unit tests
		Assume.assumeTrue(System.getProperty("os.name").startsWith("Linux") && System.getProperty("os.version").contains("alios7"));

		NiagaraUtils.ensureNiagaraIsLoaded(TEMPORARY_FOLDER.newFolder().getAbsolutePath());
	}

	@AfterClass
	public static void resetNiagara() throws Exception {
		NiagaraUtils.resetNiagaraLoadedFlag();
	}

	@After
	public void closeBackend() {
		if (backend != null) {
			backend.close();
			backend = null;
		}
	}

	@Test
	public void testReleasingNiagaraObjectsAfterBackendClosed() throws Exception {
		setupNiagaraStateBackend();

		try {
			NiagaraTabletInstance spyDB = backend.getDbInstance();

			//Ensure every NiagaraObjects not closed yet
			for (AbstractImmutableNativeReference niagaraObject : niagaraObjects) {
				assertTrue(niagaraObject.isOwningHandle());
			}

			assertNotNull(backend.tabletInstance);

			this.backend.closeImpl();

			verify(spyDB, times(1)).close();
			assertNull(backend.tabletInstance);
			assertNull(backend.db);

			//Ensure every NiagaraObjects was closed exactly once
			for (AbstractImmutableNativeReference niagaraObject : niagaraObjects) {
				assertFalse(niagaraObject.isOwningHandle());
			}

		} finally {
			backend.closeImpl();
			backend = null;
		}
	}

	@Test
	public void testCloseImplDeletesAllDirectories() throws Exception {
		setupNiagaraStateBackend();

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
	public void testDismissingSnapshot() throws Exception {
		setupNiagaraStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			verify(niagaraResourceGuard, times(1)).acquireResource();
			assertEquals(1, niagaraResourceGuard.getLeaseCount());
			assertEquals(0, testStreamFactory.getAllCreatedStreams().size());
			snapshot.cancel(true);
			assertEquals(0, niagaraResourceGuard.getLeaseCount());

		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testDismissingSnapshotNotRunnable() throws Exception {
		setupNiagaraStateBackend();

		try {
			RunnableFuture<SnapshotResult<StatePartitionSnapshot>> snapshot =
				backend.snapshot(0L, 0L, testStreamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());
			assertEquals(1, niagaraResourceGuard.getLeaseCount());
			snapshot.cancel(true);
			Thread asyncSnapshotThread = new Thread(snapshot);
			asyncSnapshotThread.start();
			try {
				snapshot.get();
				fail();
			} catch (Exception ignored) {

			}
			asyncSnapshotThread.join();
			verify(niagaraResourceGuard, times(1)).acquireResource();
			assertEquals(0, testStreamFactory.getAllCreatedStreams().size());
			assertEquals(0, niagaraResourceGuard.getLeaseCount());

		} finally {
			this.backend.closeImpl();
			this.backend = null;
		}
	}

	@Test
	public void testCompletingSnapshot() throws Exception {
		setupNiagaraStateBackend();

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
			TestCase.assertNotNull(snapshot);
			assertTrue(snapshot.getStateSize() > 0);
			assertEquals(new GroupRange(0, 10), snapshot.getGroups());

			// version file, at least one SST file, CURRENT file and empty log file.
			assertTrue(testStreamFactory.getAllCreatedStreams().size() >= 4);
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
		setupNiagaraStateBackend();
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
	public void testSharedIncrementalStateDeRegistration() throws Exception {
		if (enableIncrementalCheckpointing) {
			setupNiagaraStateBackend();

			CheckpointStorage checkpointStorage = new FsStateBackend(TEMPORARY_FOLDER.newFolder().toURI().toString()).createCheckpointStorage(new JobID());
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

	private void setupNiagaraStateBackend() throws Exception {
		blocker = new OneShotLatch();
		waiter = new OneShotLatch();
		testStreamFactory = new BlockerCheckpointStreamFactory(1024 * 1024);
		testStreamFactory.setBlockerLatch(blocker);
		testStreamFactory.setWaiterLatch(waiter);
		testStreamFactory.setAfterNumberInvocations(10);

		backend =  new NiagaraInternalStateBackend(
			ClassLoader.getSystemClassLoader(),
			TEMPORARY_FOLDER.newFolder(),
			new NiagaraConfiguration(),
			10,
			getGroupsForSubtask(10, 1, 0),
			true,
			mock(LocalRecoveryConfig.class),
			null);
		backend.restore(null);

		state1 = backend.getInternalState(stateDescriptor1);
		state2 = backend.getInternalState(stateDescriptor2);

		backend.tabletInstance = spy(backend.tabletInstance);

		// spy on final 'niagaraResourceGuard' field
		final Field resourceGuardField = NiagaraInternalStateBackend.class.getDeclaredField("niagaraResourceGuard");
		resourceGuardField.setAccessible(true);
		niagaraResourceGuard = spy(backend.niagaraResourceGuard);
		resourceGuardField.set(backend, niagaraResourceGuard);

		for (int i = 0; i < 100; i++) {
			Row internalKey1 = Row.of(i);
			Row internalKey2 = Row.of(i, ThreadLocalRandom.current().nextInt());
			state1.setCurrentGroup(partitioner.partition(internalKey1, maxParallism));
			state1.put(internalKey1, Row.of(ThreadLocalRandom.current().nextFloat()));
			state2.setCurrentGroup(partitioner.partition(internalKey2, maxParallism));
			state2.put(internalKey2, Row.of(ThreadLocalRandom.current().nextFloat()));
		}

		niagaraObjects = new ArrayList<>();

		TabletOptions tabletOptions = (TabletOptions) Whitebox.getInternalState(backend.tabletInstance, "tabletOptions");
		WriteOptions writeOptions = (WriteOptions) Whitebox.getInternalState(backend.tabletInstance, "writeOptions");
		ReadOptions readOptions = (ReadOptions) Whitebox.getInternalState(backend.tabletInstance, "readOptions");
		niagaraObjects.add(tabletOptions);
		niagaraObjects.add(writeOptions);
		niagaraObjects.add(readOptions);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				NiagaraIterator niagaraIterator = spy((NiagaraIterator) invocation.callRealMethod());
				niagaraObjects.add(niagaraIterator);
				return niagaraIterator;
			}
		}).when(backend.tabletInstance).iterator();
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
