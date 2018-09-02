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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;

/**
 * Mering policy for a list of sorted data files.
 */
public interface SortedDataFileMerger<T> {

	/**
	 * Merges a list of sorted data files.
	 *
	 * @param files the files to merge
	 * @param writeMemory the memory used to write files during merging
	 * @param mergeReadMemory the memory used to read files during merging
	 * @param channelDeleteRegistry the registry to manage files to be close and delete
	 * @param aliveFlag flags indicating whether the merge should continue
	 */
	List<SortedDataFile<T>> merge(List<SortedDataFile<T>> files,
									List<MemorySegment> writeMemory,
									List<MemorySegment> mergeReadMemory,
									ChannelDeleteRegistry<T> channelDeleteRegistry,
									BooleanValue aliveFlag) throws IOException;

	/**
	 * Gets the merging iterator to read data in-order for a list of sorted data files and an optional large records
	 * iterator.
	 *
	 * @param files the files based to create the merging iterator
	 * @param mergeReadMemory the memory used to read files during merging
	 * @param largeRecords the iterator of large records
	 * @param channelDeleteRegistry the registry to manage files to be close and delete
	 */
	MutableObjectIterator<T> getMergingIterator(List<SortedDataFile<T>> files,
												List<MemorySegment> mergeReadMemory,
												MutableObjectIterator<T> largeRecords,
												ChannelDeleteRegistry<T> channelDeleteRegistry) throws IOException;

}
