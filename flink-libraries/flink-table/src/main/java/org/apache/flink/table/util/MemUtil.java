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

package org.apache.flink.table.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Tool class to manage floating allocated memory of table operators.
 */
public class MemUtil {
	public static void releaseSpecificNumFloatingSegments(
		MemoryManager manager, List<MemorySegment> segments, int specificNum) {
		if (specificNum > 0) {
			int remain = specificNum;
			List<MemorySegment> releaseSegs = new ArrayList<>(specificNum);
			for (MemorySegment segment : segments) {
				releaseSegs.add(segment);
				if (--remain == 0) {
					break;
				}
			}
			// If remain > 0, we have not enough segments to release.
			if (remain == 0 && releaseSegs.size() == specificNum) {
				segments.removeAll(releaseSegs);
				manager.release(releaseSegs, false);
			} else {
				throw new IllegalStateException(
					"The allocated segments can't release the specific" + specificNum + " pages!");
			}
		}
	}
}
