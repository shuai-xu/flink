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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FadvisedReadAheadSynchronousBufferFileReader extends FadvisedSynchronousBufferFileReader {
	private final long maxReadAheadLength;
	private final ReadaheadPool readaheadPool;
	private ReadaheadRequest readaheadRequest = null;

	public FadvisedReadAheadSynchronousBufferFileReader(ID channelID, boolean writeEnabled,
		long startOffset, long length, long maxReadAheadLength, ReadaheadPool readaheadPool) throws IOException {

		super(channelID, writeEnabled, OsCachePolicy.READ_AHEAD, startOffset, length);
		this.maxReadAheadLength = maxReadAheadLength;
		this.readaheadPool = checkNotNull(readaheadPool);
	}

	@Override
	public void readInto(Buffer buffer, long length) throws IOException {
		readaheadRequest = readaheadPool.readaheadStream(
			fileIdentifier, fd, currentPosition, maxReadAheadLength, endOffset, readaheadRequest);
		super.readInto(buffer, length);
	}

	@Override
	public void close() throws IOException {
		if (readaheadRequest != null) {
			readaheadRequest.cancel();
			readaheadRequest = null;
		}
		super.close();
	}
}
