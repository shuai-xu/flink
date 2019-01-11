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

package org.apache.flink.table.util.resource;

import org.apache.flink.streaming.api.graph.StreamNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Util to access Steam Node.
 */
public class StreamNodeUtil {
	private static final Logger LOG = LoggerFactory.getLogger(StreamNodeUtil.class);

	/**
	 * Set max parallelism.
	 *
	 * @param streamNode StreamNode
	 * @param paral      Paral
	 */
	public static void setMaxParallelism(StreamNode streamNode, int paral) {
		try {
			Method method = StreamNode.class.getDeclaredMethod("setMaxParallelism",
					int.class);
			method.setAccessible(true);
			method.invoke(streamNode, paral);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("setMaxParallelism paral :" + paral + " failed", e);
		}
	}

	/**
	 * Get max parallelism of stream node.
	 *
	 * @param streamNode StreamNode
	 * @return Max parallelism
	 */
	public static int getMaxParallelism(StreamNode streamNode) {
		try {
			Method method = StreamNode.class.getDeclaredMethod("getMaxParallelism");
			method.setAccessible(true);
			return (int) method.invoke(streamNode);
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn("getMaxParallelism failed", e);
		}

		return -1;
	}
}

