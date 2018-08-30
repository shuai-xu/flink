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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class ExternalBlockSubpartitionView implements ResultSubpartitionView, Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockSubpartitionView.class);

	@Override
	public void run() {

	}

	@Nullable
	@Override
	public ResultSubpartition.BufferAndBacklog getNextBuffer() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public void notifyDataAvailable() {

	}

	@Override
	public void releaseAllResources() throws IOException {

	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {

	}

	@Override
	public boolean isReleased() {
		return false;
	}

	@Override
	public Throwable getFailureCause() {
		return null;
	}

	@Override
	public boolean nextBufferIsEvent() {
		return false;
	}

	@Override
	public boolean isAvailable() {
		return false;
	}

	// TODO
	//@Override
	void notifyCreditAdd(int credit) {
		// TODO if subpartition views' sorting policy is credit-based, we need to refresh queues in thread pool.
	}

	// TODO
	int getCredit() {
		return 0;
	}

	// TODO
	String getResultPartitionDir() {
		return null;
	}

	// TODO
	int getSubpartitionIndex() {
		return 0;
	}

	public static final class ExternalSubpartitionMeta {

		private final Path dataFile;

		private final long offset;

		private final long bufferNum;

		public ExternalSubpartitionMeta(
			Path dataFile, long offset, long bufferNum) {

			assert dataFile != null;
			assert offset >= 0;
			assert bufferNum >= 0;

			this.dataFile = dataFile;
			this.offset = offset;
			this.bufferNum = bufferNum;
		}

		Path getDataFile() {
			return dataFile;
		}

		long getOffset() {
			return offset;
		}

		long getBufferNum() {
			return bufferNum;
		}

		@Override
		public String toString() {
			return "{ dataFilePath = " + dataFile + ", offset = " + offset
				+ ", buffNum = " + bufferNum + " }";
		}
	}
}
