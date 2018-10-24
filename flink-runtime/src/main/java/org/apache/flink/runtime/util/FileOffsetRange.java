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

package org.apache.flink.runtime.util;

/**
 * To specify offset range of a file.
 */
public class FileOffsetRange {

	private final long start;

	private final long size;

	public FileOffsetRange(long start, long size) {
		this.start = start;
		this.size = size;
	}

	public long getStart() {
		return start;
	}

	public long getSize() {
		return size;
	}

	@Override
	public String toString() {
		return "start=" + start + ", size=" + size;
	}
}
