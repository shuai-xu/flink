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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import org.apache.flink.table.api.InputPartitionSource;

import com.alibaba.blink.launcher.autoconfig.SourcePartitionFetcher;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SourcePartitionFectcherTest {

	@Test
	public void testGetSourcePartition() throws Exception {
		SourcePartitionFetcher.getInstance().reset();
		List<String> list = new ArrayList<>();
		list.add("0");
		list.add("1");
		list.add("2");

		int sourceId = 1;
		InputPartitionSource mockSource = mock(InputPartitionSource.class);
		when(mockSource.getPartitionList()).thenReturn(list);
		SourcePartitionFetcher.getInstance().addSourceFunction(sourceId, "source1", mockSource);

		int sourceId2 = 2;
		InputPartitionSource mockSource2 = mock(InputPartitionSource.class);
		when(mockSource2.getPartitionList()).thenThrow(new IOException("failed to connect tt"));
		SourcePartitionFetcher.getInstance().addSourceFunction(sourceId2, "source2", mockSource2);

		assertEquals(3, (long) SourcePartitionFetcher.getInstance().getSourcePartitions().get(sourceId));
		assertEquals(0, (long) SourcePartitionFetcher.getInstance().getSourcePartitions().get(sourceId2));
	}
}
