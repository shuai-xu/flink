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

package org.apache.flink.api.common.io.blockcompression;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A compressor which compresses a whole byte array each time.
 * It will read from and write to byte arrays given from the outside, reducing copy time.
 */
public abstract class AbstractBlockCompressor {
	private byte[] reuseSrcHeapBuff;
	private byte[] reuseDstHeapBuff;

	public abstract int getMaxCompressedSize(int srcSize);

	public int getMaxCompressedSize(byte[] src) {
		return getMaxCompressedSize(src.length);
	}

	/**
	 * Compress data from src byte buffer and write result to dst byte buffer.
	 * Note that dst byte buffer will be cleared before filled with compressed data.
	 */
	public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
		return compress(src, 0, src.remaining(), dst);
	}

	public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst) throws IOException {
		byte[] srcArr;
		if (src.hasArray()) {
			srcArr = src.array();
			srcOff += src.arrayOffset() + src.position();
		} else {
			int len = srcOff + srcLen;
			if (reuseSrcHeapBuff == null || reuseSrcHeapBuff.length < len) {
				reuseSrcHeapBuff = new byte[len];
			}
			srcArr = reuseSrcHeapBuff;
			src.get(srcArr, 0, len);
		}

		dst.clear();

		byte[] dstArr;
		int dstOff;
		if (dst.hasArray()) {
			dstArr = dst.array();
			dstOff = dst.arrayOffset();
		} else {
			int len = dst.capacity();
			if (reuseDstHeapBuff == null || reuseDstHeapBuff.length < len) {
				reuseDstHeapBuff = new byte[len];
			}
			dstArr = reuseDstHeapBuff;
			dstOff = 0;
		}

		int compressedLen = compress(srcArr, srcOff, srcLen, dstArr, dstOff);
		if (!dst.hasArray()) {
			dst.put(dstArr, dstOff, compressedLen);
		}
		dst.position(0);
		dst.limit(compressedLen);

		return compressedLen;
	}

	public int compress(byte[] src, byte[] dst) throws IOException {
		return compress(src, 0, src.length, dst, 0);
	}

	public abstract int compress(
			byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) throws IOException;
}
