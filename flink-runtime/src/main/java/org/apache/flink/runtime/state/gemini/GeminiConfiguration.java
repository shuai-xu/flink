/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.gemini;

import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for {@link GeminiInternalStateBackend}.
 */
public class GeminiConfiguration {

	/**
	 * Prefix for all gemini-related configurations.
	 */
	public static final String STATEBACKEND_PREFIX = "state.backend.gemini.";

	/**
	 * Config parameter defining the type of snapshot to take. It can be "FULL" or "INCREMENTAL".
	 * The default value is "FULL".
	 */
	public static final String GEMINI_SNAPSHOT_TYPE = STATEBACKEND_PREFIX + "snapshot.type";

	/**
	 * Config parameter defining the type of memory to use. It can be "HEAP" or "OFFHEAP".
	 * The default value is "HEAP".
	 */
	public static final String GEMINI_MEMORY_TYPE = STATEBACKEND_PREFIX + "memory.type";

	/**
	 * Whether to copy the value when user adds or gets state. The user will hold the references of state
	 * if this parameter is "false". In this case, the state can be changed during one checkpoint, and a
	 * inconsistent snapshot may be taken. If the parameter is set to "true", user will sacrifice some
	 * performance to guarantee consistency. The default value is true.
	 */
	public static final String GEMINI_COPY_VALUE = STATEBACKEND_PREFIX + "copy.value";

	private final Map<String, String> configMap;

	public GeminiConfiguration() {
		this(null);
	}

	public GeminiConfiguration(Configuration configuration) {
		this.configMap = new HashMap<>();

		loadDefaultConfiguration();

		if (configuration != null) {
			loadConfiguration(configuration);
		}
	}

	private void loadDefaultConfiguration() {
		this.configMap.put(GEMINI_SNAPSHOT_TYPE, SnapshotType.FULL.toString());
		this.configMap.put(GEMINI_MEMORY_TYPE, MemoryType.HEAP.toString());
		this.configMap.put(GEMINI_COPY_VALUE, String.valueOf(true));
	}

	private void loadConfiguration(Configuration configuration) {
		for (String key : configMap.keySet()) {
			String value = configuration.getString(key, null);
			if (value != null) {
				configMap.put(key, value);
			}
		}
	}

	public SnapshotType getSnapshotType() {
		return SnapshotType.valueOf(configMap.get(GEMINI_SNAPSHOT_TYPE));
	}

	public MemoryType getMemoryType() {
		return MemoryType.valueOf(configMap.get(GEMINI_MEMORY_TYPE));
	}

	public boolean isCopyValue() {
		return Boolean.valueOf(configMap.get(GEMINI_COPY_VALUE));
	}

	@Override
	public String toString() {
		return "snapshotType=" + configMap.get(GEMINI_SNAPSHOT_TYPE) +
			" memoryType=" + configMap.get(GEMINI_MEMORY_TYPE) +
			" copyValue=" + configMap.get(GEMINI_COPY_VALUE);
	}

	/**
	 * The type of snapshot to take.
	 */
	public enum SnapshotType {

		/**
		 * Always take full snapshots.
		 */
		FULL,

		/**
		 * Takes incremental snapshots first, and a full snapshot may be taken
		 * to compact multiple small incremental snapshots.
		 */
		INCREMENTAL
	}

	/**
	 * The type of memory to store states.
	 */
	public enum MemoryType {

		/**
		 * Stores states on heap.
		 */
		HEAP,

		/**
		 * Stores states off heap.
		 */
		OFFHEAP
	}

}
