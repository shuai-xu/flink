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

/**
 * A factory generating instances of block compressors and decompressors
 * by the given compression method.
 */
public class BlockCompressionFactory {

	public enum CompressionMethod {
		LZ4, BZIP2, GZIP
	}

	public static AbstractBlockCompressor getCompressor(CompressionMethod method) {
		switch (method) {
			case LZ4:
				return new Lz4BlockCompressor();
			case BZIP2:
				return new Bzip2BlockCompressor();
			case GZIP:
				return new GzipBlockCompressor();
			default:
				throw new IllegalArgumentException("Unsupported compression method");
		}
	}

	public static AbstractBlockDecompressor getDecompressor(CompressionMethod method) {
		switch (method) {
			case LZ4:
				return new Lz4BlockDecompressor();
			case BZIP2:
				return new Bzip2BlockDecompressor();
			case GZIP:
				return new GzipBlockDecompressor();
			default:
				throw new IllegalArgumentException("Unsupported compression method");
		}
	}

	public static AbstractBlockCompressor getCompressor(String method) {
		switch (method.toLowerCase()) {
			case "lz4":
				return new Lz4BlockCompressor();
			case "bzip2":
				return new Bzip2BlockCompressor();
			case "gzip":
				return new GzipBlockCompressor();
			default:
				throw new IllegalArgumentException("Unsupported compression method");
		}
	}

	public static AbstractBlockDecompressor getDecompressor(String method) {
		switch (method.toLowerCase()) {
			case "lz4":
				return new Lz4BlockDecompressor();
			case "bzip2":
				return new Bzip2BlockDecompressor();
			case "gzip":
				return new GzipBlockDecompressor();
			default:
				throw new IllegalArgumentException("Unsupported compression method");
		}
	}
}
