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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

public class FileSegmentStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 1L;

	/** The file where the segment locates. */
	private final Path filePath;

	/** The start position of the snapshot data in the file. */
	private final long startPosition;

	/** The end position of the snapshot data in the file. */
	private final long endPosition;

	/** The state size of this segment. */
	private final long stateSize;

	/** Indicating whether the underlying file is closed. */
	private final boolean fileClosed;

	public FileSegmentStateHandle(
			final Path filePath,
			final long startPosition,
			final long endPosition,
			final boolean fileClosed) {

		Preconditions.checkArgument(startPosition >= 0 && endPosition >= startPosition,
			"Illegal startPosition: " + startPosition + " and endPosition:" + endPosition);

		this.filePath = filePath;
		this.startPosition = startPosition;
		this.endPosition = endPosition;
		this.stateSize = endPosition - startPosition;
		this.fileClosed = fileClosed;
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		FSDataInputStream inputStream = getFileSystem().open(filePath);
		inputStream.seek(startPosition);
		return inputStream;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FileSegmentStateHandle that = (FileSegmentStateHandle) o;

		return filePath.equals(that.filePath) &&
			startPosition == that.startPosition &&
			endPosition == that.endPosition &&
			fileClosed == that.fileClosed;
	}

	@Override
	public int hashCode() {
		int result = filePath.hashCode();
		result = 31 * result + (int) (startPosition ^ (startPosition >>> 32));
		result = 31 * result + (int) (endPosition ^ (endPosition >>> 32));
		result = 31 * result + Objects.hashCode(fileClosed);
		return result;
	}

	@Override
	public String toString() {
		return "FileSegmentStateHandle{" +
			"filePath=" + filePath +
			", startPosition=" + startPosition +
			", endPosition=" + endPosition +
			", stateSize=" + getStateSize() +
			", fileClosed=" + fileClosed +
			"}";
	}

	@Override
	public void discardState() throws Exception {
		// avoid to delete this file when part of state handle is discarded.
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	public long getStartPosition() {
		return startPosition;
	}

	public long getEndPosition() {
		return endPosition;
	}

	public Path getFilePath() {
		return filePath;
	}

	public SharedStateRegistryKey getRegistryKey() {
		return new SharedStateRegistryKey(filePath.toString());
	}

	public boolean isFileClosed() {
		return fileClosed;
	}

	private FileSystem getFileSystem() throws IOException {
		return FileSystem.get(this.filePath.toUri());
	}
}

