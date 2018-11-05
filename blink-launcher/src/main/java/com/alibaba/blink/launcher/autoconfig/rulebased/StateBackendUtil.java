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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import org.apache.flink.util.Preconditions;

import com.alibaba.blink.launcher.ConfConstants;
import com.alibaba.blink.launcher.util.NumUtil;
import com.alibaba.blink.launcher.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Calculate native memory used by state backend.
 */
public class StateBackendUtil {
	private static final Logger LOG = LoggerFactory.getLogger(StateBackendUtil.class);

	public static int getOperatorNativeMemory(Properties userProperties, boolean useState) {
		return getOperatorNativeMemory(userProperties, useState, 1);
	}

	// TODO:AUTO_CONFIG: update with Niagara state memory calculation
	/**
	 * Calculate native memory of vertex.
	 *
	 * @param userProperties User properties
	 * @param useState	Whether stateful
	 * @param statefulNodeCount	Count of stateful nodes of the vertex
	 * @return Native memory of vertex
	 */
	public static int getOperatorNativeMemory(Properties userProperties, boolean useState, int statefulNodeCount) {
		if (!useState) {
			return 0;
		}
		if (isGeminiStateBackend(userProperties)) {
			int nonStateNative = ConfConstants.DEFAULT_BLINK_NON_STATE_NATIVE_MEM_MB;
			String strNonStateNative = NumUtil.getProperty(userProperties, ResourceSettings.BLINK_NON_STATE_NATIVE_MEM_MB);
			if (strNonStateNative != null) {
				nonStateNative = NumUtil.parseInt(strNonStateNative);
				if (nonStateNative < 0) {
					nonStateNative = ConfConstants.DEFAULT_BLINK_NON_STATE_NATIVE_MEM_MB;
				}
			}
			LOG.info("non state native:" + nonStateNative);
			return nonStateNative;
		}

		Preconditions.checkArgument(statefulNodeCount > 0);
		int blockCacheSize = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
		String strBlockCache = NumUtil.getProperty(userProperties, ConfConstants.STATE_BACKEND_BLOCK_CACHE_SIZE_MB);
		if (strBlockCache != null) {
			blockCacheSize = NumUtil.parseInt(strBlockCache);
			if (blockCacheSize < 0) {
				blockCacheSize = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
			}
		}

		int memTableSize = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
		String strMemTable = NumUtil.getProperty(userProperties, ConfConstants.STATE_BACKEND_MEM_TABLE_SIZE_MB);
		if (strMemTable != null) {
			memTableSize = NumUtil.parseInt(strMemTable);
			if (memTableSize < 0) {
				memTableSize = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
			}
		}

		int taskNativeMemory;
		if (isNiagaraStateBackend(userProperties)) {
			taskNativeMemory = (int) ((blockCacheSize + memTableSize * statefulNodeCount) *
				ResourceSettings.DEFAULT_STATE_MEM_INCREASE_RATIO);
			LOG.info("blockCacheSize: {}mb, memTableSize: {}mb, stateMemIncreaseRatio: {}, taskNativeMemory: {}mb",
				blockCacheSize, memTableSize, ResourceSettings.DEFAULT_STATE_MEM_INCREASE_RATIO, taskNativeMemory);
		} else {
			taskNativeMemory = (blockCacheSize + memTableSize) * statefulNodeCount;
			LOG.info("blockCacheSize: {}mb, memTableSize: {}mb, taskNativeMemory: {}mb",
				blockCacheSize, memTableSize, taskNativeMemory);
		}

		return taskNativeMemory;
	}

	private static boolean isNiagaraStateBackend(Properties userProperties) {
		String stateBackendType = userProperties.getProperty(ConfConstants.STATE_BACKEND_TYPE);
		if (!StringUtil.isEmpty(stateBackendType) &&
			stateBackendType.equalsIgnoreCase(ConfConstants.NIAGARA)) {
			return true;
		}

		return false;
	}

	private static boolean isGeminiStateBackend(Properties userProperties) {
		String stateBackendType = userProperties.getProperty(ConfConstants.STATE_BACKEND_TYPE);
		if (StringUtil.isEmpty(stateBackendType) ||
			stateBackendType.equalsIgnoreCase(ConfConstants.GEMINI)) {
			return true;
		}

		return false;
	}
}
