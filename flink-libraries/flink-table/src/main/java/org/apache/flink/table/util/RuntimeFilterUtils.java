/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Utils for runtime filter.
 */
public class RuntimeFilterUtils {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeFilterUtils.class);

	public static CompletableFuture<BloomFilter> asyncGetBroadcastBloomFilter(
			StreamingRuntimeContext context, String broadcastId) {
		CompletableFuture<SerializedValue> future =
				context.asyncGetBroadcastAccumulator(broadcastId);
		return future.handleAsync((serializedValue, e) -> {
			if (e == null && serializedValue != null) {
				return BloomFilter.fromBytes(serializedValue.getByteArray());
			}
			if (e != null) {
				LOG.error(e.getMessage(), e);
			}
			return null;
		});
	}
}
