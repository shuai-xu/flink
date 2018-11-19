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

package com.alibaba.blink.state.niagara;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.concurrent.duration.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration for Niagara state-backend.
 */
public class NiagaraConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(NiagaraConfiguration.class);

	private static final Pattern DB_CONF_PATTERN = Pattern.compile("niagara\\..+\\..+Options\\..+");
	private static final Pattern LOG_CONF_PATTERN = Pattern.compile("niagara\\.alog\\..+");

	private final Map<String, String> entries = initConfigurationWithDefaultValue();

	/** Niagara DB configurations.*/
	private Set<String> niagaraDBConf = new HashSet<>();

	/** Niagara DB log related configurations.*/
	private Set<String> niagaraLogConf = new HashSet<>();

	public static final ConfigOption<String> LOCAL_DIRECTORIES =
		key("state.backend.niagara.datapath")
			.noDefaultValue()
			.withDescription("The directories where niagara locates, could be a string with ',' as delimiter.");

	public static final ConfigOption<String> TTL =
		key("state.backend.niagara.ttl")
			.defaultValue("3 d")
			.withDescription("The time in seconds to live for data in Niagara, the default value is 3 days.");

	public static final ConfigOption<String> BLOCK_CACHE_SIZE =
		key("state.backend.niagara.blockcache.size")
			.defaultValue("512 mb")
			.withDescription("The amount of the cache (in megabytes) for data blocks in Niagara." +
				" The default value is 512mb. And the max allowed cache-size is 1gb while the min is 64mb.");

	public static final ConfigOption<String> MEMTABLE_SIZE =
		key("state.backend.niagara.memtable.size")
			.defaultValue("64mb")
			.withDescription("The amount of memtable that will be used by a niagara tablet.");

	public static final ConfigOption<Integer> CORE_NUM =
		key("state.backend.niagara.core.num")
			.defaultValue(16)
			.withDescription("Core numbers for a Niagara instance, " +
				"the default value is 16 to ensure Niagara could use many background threads.");

	public NiagaraConfiguration() {
		// nothing to do
	}

	public NiagaraConfiguration(Configuration configuration) {
		loadConfiguration(configuration);
	}

	public String getNiagaraDBConf() {
		return niagaraDBConf.stream().filter(conf -> getInternal(conf) != null).map(conf -> conf + "=" + getInternal(conf)).collect(Collectors.joining(","));
	}

	public String getNiagaraLogConf() {
		return niagaraLogConf.stream().filter(conf -> getInternal(conf) != null).map(conf -> conf + "=" + getInternal(conf)).collect(Collectors.joining(","));
	}

	public void set(String key, String value) {
		Preconditions.checkArgument(value != null && !value.isEmpty(), "The configuration value must not be empty.");
		if (!updateConf(key, value)) {
			throw new RuntimeException("Unknown configuration for " + key + ".");
		}
	}

	//--------------------------------------------------------------------------
	// The time to live for the data in Niagara
	//--------------------------------------------------------------------------

	public int getTtl() {
		long ttlMillSeconds = Duration.apply(getInternal(TTL.key())).toMillis();
		return ttlMillSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlMillSeconds;
	}

	public NiagaraConfiguration setTtl(String ttl) {
		setInternal(TTL.key(), ttl);
		return this;
	}

	//--------------------------------------------------------------------------
	// The amount of the cache for data blocks in Niagara
	//--------------------------------------------------------------------------

	public long getBlockCacheSize() {
		return MemorySize.parseBytes(getInternal(BLOCK_CACHE_SIZE.key()));
	}

	public NiagaraConfiguration setBlockCacheSize(String blockCacheSize) {
		long blockCacheSizeBytes = MemorySize.parseBytes(blockCacheSize);
		if (blockCacheSizeBytes < 0) {
			LOG.warn("Invalid configuration for block-cache size. The value will still remain as previous value(" + getBlockCacheSize() + " bytes).");
		} else {
			setInternal(BLOCK_CACHE_SIZE.key(), blockCacheSize);
		}

		return this;
	}

	//--------------------------------------------------------------------------
	// Amount of data to build up in memory (backed by an unsorted log on disk)
	// before converting to a sorted on-disk file. Larger values increase
	// performance, especially during bulk loads.
	//--------------------------------------------------------------------------

	public long getMemtableSize() {
		return MemorySize.parseBytes(getInternal(MEMTABLE_SIZE.key()));
	}

	public NiagaraConfiguration setMemtableSize(String memtableSize) {
		long writeBufferSizeBytes = MemorySize.parseBytes(memtableSize);
		if (writeBufferSizeBytes < 0) {
			LOG.warn("Invalid configuration for memtable size. The value will still remain as previous value(" + getMemtableSize() + " bytes).");
		} else {
			setInternal(MEMTABLE_SIZE.key(), memtableSize);
		}
		return this;
	}

	//--------------------------------------------------------------------------
	// The maximum number of cores to use for compaction & flushes
	//--------------------------------------------------------------------------

	public int getCoreNum() {
		return Integer.parseInt(getInternal(CORE_NUM.key()));
	}

	public NiagaraConfiguration setCoreNum(int coreNum) {
		if (coreNum < 0) {
			LOG.warn("Invalid configuration for core numbers. The value will still remain as previous value: {}", getCoreNum());
		} else {
			setInternal(CORE_NUM.key(), Integer.toString(coreNum));
		}

		return this;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder("NiagaraConfiguration{");

		for (Map.Entry<String, String> entry : entries.entrySet()) {
			ret.append("\n\t").append(entry.getKey()).append(": ").append(entry.getValue());
		}
		ret.append("}");

		return ret.toString();
	}

	//--------------------------------------------------------------------------

	/**
	 * Returns the value in string format with the given key, if the key is mot in the predefined configurations,
	 * just throws IllegalArgumentException.
	 *
	 * @param key The configuration-key to query in string format.
	 */
	private String getInternal(String key) {
		return entries.get(key);
	}

	/**
	 * Sets the configuration with (key, value) if the key is predefined, otherwise throws IllegalArgumentException.
	 *
	 * @param key The configuration key, if key is not predefined, throws IllegalArgumentException out.
	 * @param value The configuration value.
	 */
	private void setInternal(String key, String value) {
		Preconditions.checkArgument(value != null && !value.isEmpty(),
			"The configuration value must not be empty.");

		entries.put(key, value);
	}

	public NiagaraConfiguration loadConfiguration(Configuration configuration) {
		for (String key : configuration.keySet()) {
			String value = configuration.getString(key, null);
			if (value != null) {
				updateConf(key, value.trim());
			}
		}

		return this;
	}

	/**
	 * Update key with value in Niagara configuration if this key is used within Niagara StateBackend,
	 * or match the pattern of Niagara DB conf, or match the pattern of log-related conf.
	 *
	 * @return True if updated successfully, or false if this key dose not match any legal pattern.
	 */
	private boolean updateConf(String key, String value) {
		if (value == null) {
			return false;
		}
		if (DB_CONF_PATTERN.matcher(key).find() && !entries.containsKey(key)) {
			niagaraDBConf.add(key);
			entries.put(key, value.trim());
		} else if (LOG_CONF_PATTERN.matcher(key).find() && !entries.containsKey(key)) {
			niagaraLogConf.add(key);
			entries.put(key, value.trim());
		} else if (entries.containsKey(key)) {
			String newValue = checkArgumentValid(key, value.trim());
			entries.put(key, newValue);
		} else {
			return false;
		}
		LOG.info("Update Niagara configuration {}={}", key, value);
		return true;
	}

	private static Set<String> sizeConfigSet = new HashSet<>(Arrays.asList(
		BLOCK_CACHE_SIZE.key(),
		MEMTABLE_SIZE.key()));

	private String checkArgumentValid(String key, String value) {
		try {
			if (key.equals(CORE_NUM.key())) {
				Preconditions.checkArgument(Integer.parseInt(value) > 0,
					"Configured value for key: " + key + " must be larger than 0.");
			} else if (sizeConfigSet.contains(key)) {
				Preconditions.checkArgument(MemorySize.parse(value).getBytes() > 0,
					"Configured value for key: " + key + " must be larger than 0.");
			} else if (key.equals(TTL.key())) {
				Preconditions.checkArgument(Duration.apply(value).toMillis() > 0,
					"Configured value for key: " + key + " must be larger than 0.");
			}
		} catch (Exception e) {
			throw new IllegalConfigurationException("Invalid configuration key: " + key + ", value: " + value);
		}
		return value;
	}

	private static Map<String, String> initConfigurationWithDefaultValue() {
		ConfigOption[] entries = new ConfigOption[]{
			LOCAL_DIRECTORIES,
			TTL,
			BLOCK_CACHE_SIZE,
			MEMTABLE_SIZE,
			CORE_NUM
		};
		Map<String, String> result = new HashMap<>(entries.length);
		Arrays.stream(entries).forEach(e -> {
			if (e.hasDefaultValue()) {
				result.put(e.key(), String.valueOf(e.defaultValue()));
			} else {
				result.put(e.key(), null);
			}
		});
		return result;
	}
}
