/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileSegmentStateHandle;

/**
 * A placeholder state handle for shared state that will replaced by an original that was
 * created in a previous checkpoint. This class can only be used in the referenced states of
 * {@link IncrementalSegmentStateSnapshot}.
 */
public class PlaceholderSegmentStateHandle extends PlaceholderStreamStateHandle {

	private static final long serialVersionUID = 1L;

	private final Path filePath;

	/** The start position of the snapshot data in the file. */
	private final long startPosition;

	/** The end position of the snapshot data in the file.*/
	private final long endPosition;

	/** The size of this segment. */
	private final long segmentSize;

	private final boolean fileClosed;

	public PlaceholderSegmentStateHandle(FileSegmentStateHandle fileSegmentStateHandle) {
		this(
				fileSegmentStateHandle.getFilePath(),
				fileSegmentStateHandle.getStartPosition(),
				fileSegmentStateHandle.getEndPosition(),
				fileSegmentStateHandle.isFileClosed());
	}

	private PlaceholderSegmentStateHandle(
		final Path filePath,
		final long startPosition,
		final long endPosition,
		final boolean fileClosed) {

		this.filePath = filePath;
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.segmentSize = endPosition - startPosition;
		this.fileClosed = fileClosed;
	}

	/**
	 * As a place holder stream, just return zero representing no duplicate uploaded.
	 */
	@Override
	public long getStateSize() {
		return 0L;
	}

	@Override
	public long getFullStateSize() {
		return this.segmentSize;
	}

	public Path getFilePath() {
		return filePath;
	}

	public long getStartPosition() {
		return startPosition;
	}

	public long getEndPosition() {
		return endPosition;
	}

	public FileSegmentStateHandle toFileSegmentStateHandle() {
		return new FileSegmentStateHandle(filePath, startPosition, endPosition, fileClosed);
	}
}
