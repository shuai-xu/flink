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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ExternalBlockResultPartitionManager.class, LocalResultPartitionResolverFactory.class})
public class ExternalBlockResultPartitionManagerTest {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockResultPartitionManagerTest.class);

	private final ExternalBlockShuffleServiceConfiguration externalBlockShuffleServiceConfiguration =
		mock(ExternalBlockShuffleServiceConfiguration.class);

	private ExternalBlockResultPartitionManager resultPartitionManager;

	private final LocalResultPartitionResolver localResultPartitionResolver = mock(LocalResultPartitionResolver.class);

	private final Map<String, String> dirToDiskType;

	private final Map<String, Integer> diskTypeToIOThreadNum;

	private final long consumedPartitionTTL = 600000L;

	private final long partialConsumedPartitionTTL = 3600000L;

	/** Map from ResultPartitionID to root Directory and result partition directory. */
	private final Map<ResultPartitionID, Tuple2<String, String>> resultPartitionFileInfoMap = new HashMap<>();

	enum ResultPartitionState {

		CONSUMED_NO_REFERENCE(false, false),

		CONSUMED_HAS_REFERENCE(false, true),

		PARTIAL_CONSUMED_NO_REFERENCE(true, false),

		PARTIAL_CONSUMED_HAS_REFERENCE(true, true);

		private final boolean isPartialConsumed;

		private final boolean hasReference;

		ResultPartitionState(boolean isPartialConsumed, boolean hasReference) {
			this.isPartialConsumed = isPartialConsumed;
			this.hasReference = hasReference;
		}
	}

	public ExternalBlockResultPartitionManagerTest() {
		this.dirToDiskType = new HashMap<String, String>() {{
			put("/local-dir1/", "SSD");
			put("/local-dir2/", "SSD");
			put("/local-dir3/", "HDD");
			put("/local-dir4/", ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE);
		}};

		this.diskTypeToIOThreadNum = new HashMap<String, Integer>() {{
			put("SSD", 30);
			put("HDD", 4);
			put(ExternalBlockShuffleServiceConfiguration.DEFAULT_DISK_TYPE, 1);
		}};
	}

		@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		when(externalBlockShuffleServiceConfiguration.getConfiguration()).thenReturn(configuration);
		when(externalBlockShuffleServiceConfiguration.getDiskScanIntervalInMS()).thenReturn(3600000L);
		when(externalBlockShuffleServiceConfiguration.getBufferNumber()).thenReturn(100);
		when(externalBlockShuffleServiceConfiguration.getMemorySizePerBufferInBytes()).thenReturn(4096);
		when(externalBlockShuffleServiceConfiguration.getDirToDiskType()).thenReturn(dirToDiskType);
		when(externalBlockShuffleServiceConfiguration.getDiskTypeToIOThreadNum()).thenReturn(diskTypeToIOThreadNum);
		when(externalBlockShuffleServiceConfiguration.getWaitCreditDelay()).thenReturn(2L);
		when(externalBlockShuffleServiceConfiguration.getConsumedPartitionTTL()).thenReturn(consumedPartitionTTL);
		when(externalBlockShuffleServiceConfiguration.getPartialConsumedPartitionTTL()).thenReturn(partialConsumedPartitionTTL);

		mockStatic(System.class);

		mockStatic(LocalResultPartitionResolverFactory.class);
		when(LocalResultPartitionResolverFactory.create(any(ExternalBlockShuffleServiceConfiguration.class)))
			.thenReturn(localResultPartitionResolver);
		doAnswer(invocation -> {
			ResultPartitionID resultPartitionID = invocation.getArgumentAt(0, ResultPartitionID.class);
			Tuple2<String, String> rootDirAndPartitionDir = resultPartitionFileInfoMap.get(resultPartitionID);
			if (rootDirAndPartitionDir != null) {
				return rootDirAndPartitionDir;
			} else {
				throw new IOException("Cannot find result partition " + resultPartitionID);
			}
		}).when(localResultPartitionResolver).getResultPartitionDir(any(ResultPartitionID.class));

		resultPartitionManager = spy(new ExternalBlockResultPartitionManager(externalBlockShuffleServiceConfiguration));
	}

	@After
	public void tearDown() {
		if (resultPartitionManager != null) {
			resultPartitionManager.stop();
		}
	}

	@Test
	public void testConstructor() {
		assertEquals(externalBlockShuffleServiceConfiguration.getBufferNumber(),
			(Integer) resultPartitionManager.bufferPool.getNumBuffers());
		assertEquals(externalBlockShuffleServiceConfiguration.getMemorySizePerBufferInBytes(),
			(Integer) resultPartitionManager.bufferPool.getMemorySegmentSize());

		dirToDiskType.forEach((dir, diskType) -> {
			Integer expectedTheadNum = diskTypeToIOThreadNum.get(diskType);
			assertTrue("Thread pool for dir " + dir, resultPartitionManager.dirToThreadPool.containsKey(dir));
			assertEquals(expectedTheadNum, (Integer) resultPartitionManager.dirToThreadPool.get(dir).getCorePoolSize());
		});
	}

	@Test
	public void testInitializeAndStopApplication() {
		resultPartitionManager.initializeApplication("user", "flinkStreamingJob1");
		verify(localResultPartitionResolver, times(1))
			.initializeApplication("user", "flinkStreamingJob1");
		resultPartitionManager.initializeApplication("user", "flinkStreamingJob2");
		verify(localResultPartitionResolver, times(1))
			.initializeApplication("user", "flinkStreamingJob2");

		when(localResultPartitionResolver.stopApplication("flinkStreamingJob1"))
			.thenReturn(Collections.EMPTY_SET);
		resultPartitionManager.stopApplication("flinkStreamingJob1");
		verify(localResultPartitionResolver, times(1)).stopApplication("flinkStreamingJob1");
		when(localResultPartitionResolver.stopApplication("flinkStreamingJob2"))
			.thenReturn(Collections.EMPTY_SET);
		resultPartitionManager.stopApplication("flinkStreamingJob2");
		verify(localResultPartitionResolver, times(1)).stopApplication("flinkStreamingJob2");
	}

	@Test
	public void testBasicProcess() {
		assertEquals(0, resultPartitionManager.resultPartitionMetaMap.size());

		// Tests the creation of result partition meta.
		createResultPartitions(6);
		resultPartitionFileInfoMap.forEach((resultPartitionID, rootDirAndPartitionDir) -> {
			assertTrue(resultPartitionID.toString(),
				!resultPartitionManager.resultPartitionMetaMap.contains(resultPartitionID));
			try {
				ResultSubpartitionView resultSubpartitionView = resultPartitionManager.createSubpartitionView(
					resultPartitionID, 0, mock(BufferAvailabilityListener.class));
				assertTrue(resultSubpartitionView != null);
			} catch (IOException e) {
				assertTrue("Unexpected exception: " + e, false);
			}
			assertTrue(resultPartitionID.toString(),
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
			ExternalBlockResultPartitionMeta resultPartitionMeta =
				resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID);
			assertEquals(rootDirAndPartitionDir,
				new Tuple2<>(resultPartitionMeta.getRootDir(), resultPartitionMeta.getResultPartitionDir()));
			assertEquals(1, resultPartitionMeta.getReferenceCount());
		});

		// Tests reference count.
		ResultPartitionID resultPartitionID = resultPartitionFileInfoMap.keySet().iterator().next();
		Tuple2<String, String> rootDirAndPartitionDir = resultPartitionFileInfoMap.get(resultPartitionID);
		for (int i = 0; i < 5; i++) {
			try {
				ResultSubpartitionView resultSubpartitionView = resultPartitionManager.createSubpartitionView(
					resultPartitionID, 0, mock(BufferAvailabilityListener.class));
				assertTrue(resultSubpartitionView != null);
			} catch (IOException e) {
				assertTrue("Unexpected exception: " + e, false);
			}
			assertTrue(resultPartitionID.toString(),
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
			ExternalBlockResultPartitionMeta resultPartitionMeta =
				resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID);
			assertEquals(rootDirAndPartitionDir,
				new Tuple2<>(resultPartitionMeta.getRootDir(), resultPartitionMeta.getResultPartitionDir()));
			assertEquals(2 + i, resultPartitionMeta.getReferenceCount());
		}
	}

	@Test
	public void testRecycleByTTL() {
		long baseTime = 1000L;
		int cntPerState = 2;

		when(System.currentTimeMillis()).thenReturn(baseTime);
		String[] localDirArray = dirToDiskType.keySet().toArray(new String[dirToDiskType.size()]);
		Map<ResultPartitionState, Set<ResultPartitionID>> stateToResultPartitionIDs = new HashMap<>();
		for (ResultPartitionState state : ResultPartitionState.values()) {
			stateToResultPartitionIDs.put(state, new HashSet<>());
			Random random = new Random();
			for (int i = 0; i < cntPerState; i++) {
				String rootDir = localDirArray[Math.abs(random.nextInt()) % localDirArray.length];
				String partitionDir = rootDir + "partition" + i + "/";
				ResultPartitionID resultPartitionID = new ResultPartitionID();
				resultPartitionFileInfoMap.put(resultPartitionID, new Tuple2<>(rootDir, partitionDir));

				stateToResultPartitionIDs.get(state).add(resultPartitionID);

				// Mock ExternalBlockResultPartitionMeta for better control.
				ExternalBlockResultPartitionMeta resultPartitionMeta = mock(ExternalBlockResultPartitionMeta.class);
				when(resultPartitionMeta.hasInitialized()).thenReturn(true);
				when(resultPartitionMeta.getRootDir()).thenReturn(rootDir);
				when(resultPartitionMeta.getResultPartitionDir()).thenReturn(partitionDir);
				when(resultPartitionMeta.getLastActiveTimeInMs()).thenReturn(baseTime);
				if (state.hasReference) {
					when(resultPartitionMeta.getReferenceCount()).thenReturn(2);
				} else {
					when(resultPartitionMeta.getReferenceCount()).thenReturn(0);
				}
				if (state.isPartialConsumed) {
					when(resultPartitionMeta.getUnconsumedSubpartitionCount()).thenReturn(3);
				} else {
					when(resultPartitionMeta.getUnconsumedSubpartitionCount()).thenReturn(0);
				}
				resultPartitionManager.resultPartitionMetaMap.put(resultPartitionID, resultPartitionMeta);
			}
		}

		when(System.currentTimeMillis()).thenReturn(baseTime + consumedPartitionTTL - 1);
		triggerRecycling();
		resultPartitionFileInfoMap.forEach((resultPartitionID, fileInfo) -> {
			assertTrue("ResultPartition should not be recycled, " + resultPartitionID,
				resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
		});
		verify(localResultPartitionResolver, never()).recycleResultPartition(any(ResultPartitionID.class));

		when(System.currentTimeMillis()).thenReturn(baseTime + consumedPartitionTTL + 1);
		triggerRecycling();
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			if (state.hasReference || state.equals(ResultPartitionState.PARTIAL_CONSUMED_NO_REFERENCE)) {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should not be recycled, " + resultPartitionID,
						resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			} else {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should be recycled, " + resultPartitionID,
						!resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			}
		});
		verify(localResultPartitionResolver, times(cntPerState)).recycleResultPartition(any(ResultPartitionID.class));

		when(System.currentTimeMillis()).thenReturn(baseTime + partialConsumedPartitionTTL + 1);
		triggerRecycling();
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			if (state.hasReference) {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should not be recycled, " + resultPartitionID,
						resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			} else {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should be recycled, " + resultPartitionID,
						!resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			}
		});
		verify(localResultPartitionResolver, times(cntPerState * 2)).recycleResultPartition(any(ResultPartitionID.class));

		// Dereference all the references.
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			if (state.hasReference) {
				resultPartitionIDs.forEach(resultPartitionID -> {
					ExternalBlockResultPartitionMeta resultPartitionMeta =
						resultPartitionManager.resultPartitionMetaMap.get(resultPartitionID);
					when(resultPartitionMeta.getReferenceCount()).thenReturn(0);
				});
			}
		});

		when(System.currentTimeMillis()).thenReturn(baseTime + consumedPartitionTTL + 1);
		triggerRecycling();
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			if (state.equals(ResultPartitionState.PARTIAL_CONSUMED_HAS_REFERENCE)) {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should not be recycled, " + resultPartitionID,
						resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			} else {
				resultPartitionIDs.forEach(resultPartitionID -> {
					assertTrue("ResultPartition should be recycled, " + resultPartitionID,
						!resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
				});
			}
		});
		verify(localResultPartitionResolver, times(cntPerState * 3)).recycleResultPartition(any(ResultPartitionID.class));

		when(System.currentTimeMillis()).thenReturn(baseTime + partialConsumedPartitionTTL + 1);
		triggerRecycling();
		stateToResultPartitionIDs.forEach((state, resultPartitionIDs) -> {
			resultPartitionIDs.forEach(resultPartitionID -> {
				assertTrue("ResultPartition should be recycled, " + resultPartitionID,
					!resultPartitionManager.resultPartitionMetaMap.containsKey(resultPartitionID));
			});
		});
		verify(localResultPartitionResolver, times(cntPerState * 4)).recycleResultPartition(any(ResultPartitionID.class));
	}

	// ******************************** Test Utilities *************************************

	private void createResultPartitions(int cnt) {
		String[] localDirArray = dirToDiskType.keySet().toArray(new String[dirToDiskType.size()]);
		for (int i = 0; i < cnt; i++) {
			Random random = new Random();
			String localDir = localDirArray[Math.abs(random.nextInt()) % localDirArray.length];
			// We don't really care about root dir and partition dir since we don't test read/write in this unittest.
			resultPartitionFileInfoMap.put(new ResultPartitionID(),
				new Tuple2<>(localDir, localDir + "partition" + i + "/"));
		}
	}

	void triggerRecycling() {
		resultPartitionManager.recycleResultPartitions();
	}
}
