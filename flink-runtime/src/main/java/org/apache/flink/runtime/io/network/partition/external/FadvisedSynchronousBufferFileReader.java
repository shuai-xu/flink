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

import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

public class FadvisedSynchronousBufferFileReader extends SynchronousBufferFileReader {
	protected final OsCachePolicy osCachePolicy;
	protected final long startOffset;
	protected final long endOffset;
	protected final String fileIdentifier;

	public FadvisedSynchronousBufferFileReader(ID channelID, boolean writeEnabled,
		OsCachePolicy osCachePolicy, long startOffset, long length) throws IOException {

		// Only supports reading without headers because if each buffer is written with a header
		// to suggest its length, the overall data length needed to read is unknown before reading,
		// thus there is no way to give OS hints about file cache.
		super(channelID, writeEnabled, false);

		checkArgument(!osCachePolicy.equals(OsCachePolicy.NO_TREATMENT));
		this.osCachePolicy = osCachePolicy;
		this.startOffset = startOffset;
		this.endOffset = startOffset + length;
		this.fileIdentifier = id.toString();

		if (!osCachePolicy.equals(OsCachePolicy.READ_AHEAD) && (endOffset - startOffset) > 0) {
			try {
				NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
					fileIdentifier, fd, startOffset, endOffset - startOffset, osCachePolicy.getFadviceFlag());
			} catch (Throwable t) {
				LOG.warn("Failed to manage OS cache for " + fileIdentifier + " , os cache policy "
					+ osCachePolicy.toString(), t);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if ((endOffset - startOffset) > 0) {
			try {
				NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
					fileIdentifier, fd, startOffset, endOffset - startOffset, POSIX_FADV_DONTNEED);
			} catch (Throwable t) {
				LOG.warn("Failed to manage OS cache for " + fileIdentifier + ", fadvise hint POSIX_FADV_DONTNEED", t);
			}
		}
		super.close();
	}
}
