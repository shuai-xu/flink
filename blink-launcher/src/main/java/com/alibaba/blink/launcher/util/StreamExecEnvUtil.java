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

package com.alibaba.blink.launcher.util;

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import com.alibaba.blink.launcher.ConfConstants;
import com.alibaba.blink.state.niagara.NiagaraConfiguration;
import com.alibaba.blink.state.niagara.NiagaraStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * Utils to update config of stream execution environment.
 */
public class StreamExecEnvUtil {
	private static final Logger LOG = LoggerFactory.getLogger(StreamExecEnvUtil.class);

	/**
	 * Log key and value.
	 *
	 * @param k Key
	 * @param v Value
	 */
	public static void log(String k, String v) {
		String msg = "[streamEnv] update k=" + k + ", v=" + v;
		System.out.println(msg);
	}

	/**
	 * Set user config for StreamExecutionEnvironment.
	 *
	 * @param env        StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws CliArgsException
	 */
	public static void setConfig(StreamExecutionEnvironment env, Properties userParams) throws Exception {
		setMaxParallelism(env, userParams);
		setStreamExecutionConfig(env, userParams);
		setStreamCheckpoint(env, userParams);
		setStateBackend(env, userParams);
		setStreamTimeCharacteristic(env, userParams);
	}

	/**
	 * Set max parallelism.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws CliArgsException
	 */
	private static void setMaxParallelism(StreamExecutionEnvironment streamEnv, Properties userParams) throws CliArgsException {

		String strMaxParall = userParams.getProperty(ConfConstants.STREAM_ENV_MAX_PARALLELISM);
		if (strMaxParall != null) {
			int maxParall = NumUtil.parseInt(strMaxParall);
			if (maxParall < 0) {
				throw new CliArgsException(ConfConstants.STREAM_ENV_MAX_PARALLELISM + ":"
					+ strMaxParall + ", the value must be int!");
			}
			streamEnv.setMaxParallelism(maxParall);
			log(ConfConstants.STREAM_ENV_MAX_PARALLELISM, strMaxParall);
		}
	}

	/**
	 * Set checkpoint interval.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws CliArgsException Parse args error
	 */
	public static void setStreamCheckpoint(
		StreamExecutionEnvironment streamEnv,
		Properties userParams) throws CliArgsException {
		CheckpointConfig config = streamEnv.getCheckpointConfig();
		String strInterval = userParams.getProperty(ConfConstants.CHECKPOINT_INTERVAL_MS);
		if (strInterval != null) {
			long interval = NumUtil.parseLong(strInterval);
			if (interval < 0) {
				throw new CliArgsException(ConfConstants.CHECKPOINT_INTERVAL_MS + ":" + strInterval
					+ ", the value must be long!");
			}
			config.setCheckpointInterval(interval);
			log(ConfConstants.CHECKPOINT_INTERVAL_MS, strInterval);
		}

		// checkpoint mode
		String cpMode = userParams.getProperty(ConfConstants.CHECKPOINT_MODE);
		if (cpMode != null) {
			if (cpMode.equalsIgnoreCase("AT_LEAST_ONCE")) {
				config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
				log(ConfConstants.CHECKPOINT_MODE, "AT_LEAST_ONCE");
			} else if (cpMode.equalsIgnoreCase("EXACTLY_ONCE")) {
				config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
				log(ConfConstants.CHECKPOINT_MODE, "EXACTLY_ONCE");
			} else if (cpMode.equalsIgnoreCase("BATCH")) {
				config.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
				log(ConfConstants.CHECKPOINT_MODE, "BATCH");
			} else {
				LOG.warn("ignore unknown checkpoint mode:" + cpMode);
			}
		}

		// checkpoint fail on error
		String cpFailOnError = userParams.getProperty(ConfConstants.CHECKPOINT_FAIL_ON_ERROR,
			ConfConstants.DEFAULT_CHECKPOINT_FAIL_ON_ERROR);
		if (!StringUtil.isEmpty(cpFailOnError)) {
			if (cpFailOnError.equalsIgnoreCase("true")) {
				config.setFailOnCheckpointingErrors(true);
				log(ConfConstants.CHECKPOINT_FAIL_ON_ERROR, "true");
			} else if (cpFailOnError.equalsIgnoreCase("false")) {
				config.setFailOnCheckpointingErrors(false);
				log(ConfConstants.CHECKPOINT_FAIL_ON_ERROR, "false");
			} else {
				LOG.warn("ignore unknown args, checkpoint fail on error: " + cpFailOnError);
			}
		}

		// checkpoint timeout
		String strTimeout = userParams.getProperty(ConfConstants.CHECKPOINT_TIMEOUT_MS);
		if (strTimeout != null) {
			long timeout = NumUtil.parseLong(strTimeout);
			if (timeout < 0) {
				throw new CliArgsException(ConfConstants.CHECKPOINT_TIMEOUT_MS + ":" + strTimeout
					+ ", the value must be long!");
			}
			config.setCheckpointTimeout(timeout);
			log(ConfConstants.CHECKPOINT_TIMEOUT_MS, strTimeout);
		}

		// checkpoint max concurrent
		String strMaxConcurrent = userParams.getProperty(ConfConstants.CHECKPOINT_MAX_CONCURRENT);
		if (strMaxConcurrent != null) {
			int maxConcurrent = NumUtil.parseInt(strMaxConcurrent);
			if (maxConcurrent < 0) {
				throw new CliArgsException(ConfConstants.CHECKPOINT_MAX_CONCURRENT + ":"
					+ strMaxConcurrent + ", the value must be int!");
			}

			Preconditions.checkArgument(maxConcurrent == 1, "Do not support concurrent checkpoint.");
			config.setMaxConcurrentCheckpoints(maxConcurrent);
			log(ConfConstants.CHECKPOINT_MAX_CONCURRENT, strMaxConcurrent);
		}

		// checkpoint min pause
		String strMinPause = userParams.getProperty(ConfConstants.CHECKPOINT_MIN_PAUSE_MS);
		if (strMinPause != null) {
			long minPause = NumUtil.parseLong(strMinPause);
			if (minPause < 0) {
				throw new CliArgsException(ConfConstants.CHECKPOINT_MIN_PAUSE_MS + ":" + strMinPause
					+ ", the value must be long!");
			}
			config.setMinPauseBetweenCheckpoints(minPause);
			log(ConfConstants.CHECKPOINT_MIN_PAUSE_MS, strMinPause);
		}
	}

	/**
	 * Set execution config, e.g. global parameters, warkmark interval...
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User parameters
	 * @throws CliArgsException Parse args error
	 */
	private static void setStreamExecutionConfig(
		StreamExecutionEnvironment streamEnv,
		Properties userParams) throws CliArgsException {
		// global job parameters
		Map<String, String> map = new HashMap<>();
		GlobalJobParameters currentConfig = streamEnv.getConfig().getGlobalJobParameters();
		if (null != currentConfig) {
			map.putAll(currentConfig.toMap());
		}

		for (String name : userParams.stringPropertyNames()) {
			map.put(name, userParams.getProperty(name));
		}

		streamEnv.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));

		// auto watermark interval
		String strWaterMarkInterval = userParams.getProperty(
			ConfConstants.AUTO_WATERMARK_INTERVAL_MS);
		if (strWaterMarkInterval != null) {
			long waterMarkInterval = NumUtil.parseLong(strWaterMarkInterval);
			if (waterMarkInterval < 0) {
				throw new CliArgsException(ConfConstants.AUTO_WATERMARK_INTERVAL_MS +
					":" + strWaterMarkInterval + ", the value must be long!");
			}
			streamEnv.getConfig().setAutoWatermarkInterval(waterMarkInterval);
			log(ConfConstants.AUTO_WATERMARK_INTERVAL_MS, strWaterMarkInterval);
		}
	}

	/**
	 * Set state backend.
	 *
	 * @param env  		 StreamExecutionEnvironment
	 * @param userParams User parameters
	 * @throws Exception Set state backend failed
	 */
	public static void setStateBackend(StreamExecutionEnvironment env, Properties userParams) throws Exception {
		// set state backend
		String stateBackendType = userParams.getProperty(ConfConstants.STATE_BACKEND_TYPE);
		if (StringUtil.isEmpty(stateBackendType)) {
			if (userParams.containsKey(ConfConstants.STATE_BACKEND_NIAGARA_TTL_MS)) {
				userParams.setProperty(ConfConstants.STATE_BACKEND_TYPE, ConfConstants.NIAGARA);
				stateBackendType = ConfConstants.NIAGARA;
			} else if (userParams.containsKey(ConfConstants.STATE_BACKEND_ROCKSDB_TTL_MS)) {
				userParams.setProperty(ConfConstants.STATE_BACKEND_TYPE, ConfConstants.ROCKSDB);
				stateBackendType = ConfConstants.ROCKSDB;
			}
		}

		if (!StringUtil.isEmpty(stateBackendType)) {
			LOG.info(ConfConstants.STATE_BACKEND_TYPE + " : " + stateBackendType);
			if (stateBackendType.equalsIgnoreCase(ConfConstants.GEMINI)) {
				setStreamGemini(env, userParams);
				return;
			} else if (stateBackendType.equalsIgnoreCase(ConfConstants.ROCKSDB)) {
				setStreamRocksDB(env, userParams);
				return;
			} else if (stateBackendType.equalsIgnoreCase(ConfConstants.NIAGARA)) {
				setStreamNiagara(env, userParams);
				return;
			} else {
				LOG.warn("Unknown state backend type: " + stateBackendType);
			}
		}
	}

	/**
	 * Set Niagara backend.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws Exception Set niagara backend failed
	 */
	private static void setStreamNiagara(
		StreamExecutionEnvironment streamEnv,
		Properties userParams)
		throws Exception {

		String strTTL = userParams.getProperty(ConfConstants.STATE_BACKEND_NIAGARA_TTL_MS);
		long ttl = NumUtil.parseLong(strTTL);
		if (ttl < 0) {
			throw new CliArgsException(ConfConstants.STATE_BACKEND_NIAGARA_TTL_MS +
					":" + strTTL + ", the value must be long!");
		} else {
			NiagaraConfiguration configuration = new NiagaraConfiguration();
			// set ttl
			configuration.setTtl(ttl + "ms"); // append time unit
			log(NiagaraConfiguration.TTL.key(), ttl + "ms");

			// set block cache size
			int blockCacheSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
			String strBlockCache = userParams.getProperty(ConfConstants.STATE_BACKEND_BLOCK_CACHE_SIZE_MB);
			if (strBlockCache != null) {
				blockCacheSizeMb = NumUtil.parseInt(strBlockCache);
				if (blockCacheSizeMb < 0) {
					blockCacheSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
				}
			}
			configuration.setBlockCacheSize(blockCacheSizeMb + "mb");
			log(NiagaraConfiguration.BLOCK_CACHE_SIZE.key(), blockCacheSizeMb + "mb");

			// set mem table size
			int memTableSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
			String strMemTable = userParams.getProperty(ConfConstants.STATE_BACKEND_MEM_TABLE_SIZE_MB);
			if (strMemTable != null) {
				memTableSizeMb = NumUtil.parseInt(strMemTable);
				if (memTableSizeMb < 0) {
					memTableSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
				}
			}
			configuration.setMemtableSize(memTableSizeMb + "mb");
			log(NiagaraConfiguration.MEMTABLE_SIZE.key(), memTableSizeMb + "mb");

			String checkPointPath = userParams.getProperty(ConfConstants.CHECKPOINT_PATH);
			NiagaraStateBackend backend = null;
			if (checkPointPath == null) {
				backend = new NiagaraStateBackend(
						new MemoryStateBackend(), TernaryBoolean.TRUE, configuration);
			} else {
				backend = new NiagaraStateBackend(
						checkPointPath, true, configuration);
			}
			streamEnv.setStateBackend(backend);
		}
	}

	/**
	 * Set RocksDB backend.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws Exception Set RocksDB backend failed
	 */
	private static void setStreamRocksDB(
		StreamExecutionEnvironment streamEnv,
		Properties userParams)
		throws Exception {

		RocksDBConfiguration configuration = new RocksDBConfiguration();
		String strTTL = userParams.getProperty(ConfConstants.STATE_BACKEND_ROCKSDB_TTL_MS);
		long ttl = NumUtil.parseLong(strTTL);
		if (ttl < 0) {
			throw new CliArgsException(ConfConstants.STATE_BACKEND_ROCKSDB_TTL_MS +
					":" + strTTL + ", the value must be long!");
		} else {
			LOG.info("set state backend as rocksdb");

			// set ttl
			configuration.setTtl(ttl + "ms"); // append time unit
			log(RocksDBConfiguration.TTL.key(), ttl + "ms");

			// set block cache size
			int blockCacheSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
			blockCacheSizeMb = NumUtil.getProperty(
					userParams, ConfConstants.STATE_BACKEND_BLOCK_CACHE_SIZE_MB, blockCacheSizeMb);
			if (blockCacheSizeMb < 0) {
				blockCacheSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_BLOCK_CACHE_SIZE_MB;
			}
			configuration.setBlockCacheSize(blockCacheSizeMb + "mb");
			log(RocksDBConfiguration.BLOCK_CACHE_SIZE.key(), blockCacheSizeMb + "mb");

			// set number of mem table
			int memTableNum = ConfConstants.DEFAULT_STATE_BACKEND_ROCKSDB_MEM_TABLE_NUM;
			memTableNum = NumUtil.getProperty(
					userParams,
					ConfConstants.STATE_BACKEND_ROCKSDB_MEM_TABLE_NUM,
					memTableNum);
			if (memTableNum < 0) {
				memTableNum = ConfConstants.DEFAULT_STATE_BACKEND_ROCKSDB_MEM_TABLE_NUM;
			}

			configuration.setMaxWriteBufferNumber(memTableNum);
			log(RocksDBConfiguration.WRITE_BUFFER_NUMBER.key(), String.valueOf(memTableNum));

			// set each mem table size
			int memTableTotalSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
			String strMemTable = userParams.getProperty(ConfConstants.STATE_BACKEND_MEM_TABLE_SIZE_MB);
			if (strMemTable != null) {
				memTableTotalSizeMb = NumUtil.parseInt(strMemTable);
				if (memTableTotalSizeMb < 0) {
					memTableTotalSizeMb = ConfConstants.DEFAULT_STATE_BACKEND_MEM_TABLE_SIZE_MB;
				}
			}
			int menTableSizeMb = memTableTotalSizeMb / memTableNum;
			configuration.setWriteBufferSize(menTableSizeMb + "mb");
			log(RocksDBConfiguration.WRITE_BUFFER_SIZE.key(), menTableSizeMb + "mb");
		}
		String checkPointPath = userParams.getProperty(ConfConstants.CHECKPOINT_PATH);
		RocksDBStateBackend backend = null;
		if (checkPointPath == null) {
			backend = new RocksDBStateBackend(new MemoryStateBackend(), true);
		} else {
			backend = new RocksDBStateBackend(checkPointPath, true);
		}
		backend.setOptions(configuration);
		streamEnv.setStateBackend(backend);
	}

	/**
	 * Set Gemini backend.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws Exception Set niagara backend failed
	 */
	private static void setStreamGemini(
		StreamExecutionEnvironment streamEnv,
		Properties userParams) throws Exception {
		throw new CliArgsException("Gemini state backend not supported");
	}

	/**
	 * Set TimeCharacteristic.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws CliArgsException Parse args error
	 */
	private static void setStreamTimeCharacteristic(
		StreamExecutionEnvironment streamEnv,
		Properties userParams) throws CliArgsException {
		String recordTimestampType = userParams.getProperty(ConfConstants.RECORD_TIMESTAMP_TYPE);
		if (recordTimestampType == null) {
			LOG.debug("no TimeCharacteristic update of stream env");
			return;
		}

		if (recordTimestampType.equalsIgnoreCase("ProcessingTime")) {
			streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
			log("StreamTimeCharacteristic", "ProcessingTime");
		} else if (recordTimestampType.equalsIgnoreCase("IngestionTime")) {
			streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			log("StreamTimeCharacteristic", "IngestionTime");
		} else if (recordTimestampType.equalsIgnoreCase("EventTime")) {
			streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			log("StreamTimeCharacteristic", "EventTime");
		} else {
			LOG.error("Unknown recordTimestampType: " + recordTimestampType
				+ ", only support ProcessingTime, IngestionTime and EventTime");
			throw new CliArgsException("unknown type: " + recordTimestampType);
		}
	}

	/**
	 * Set stream object reuse.
	 *
	 * @param streamEnv  StreamExecutionEnvironment
	 * @param userParams User params
	 * @throws CliArgsException Parse args error
	 */
	public static void setStreamEnvConfigs(
		StreamExecutionEnvironment streamEnv,
		Properties userParams) throws Exception {

		String objectReuse = userParams.getProperty(ConfConstants.OBJECT_REUSE, "true");
		if ("true".equalsIgnoreCase(objectReuse)) {
			LOG.debug("enable object reuse");
			streamEnv.getConfig().enableObjectReuse();
			log(ConfConstants.OBJECT_REUSE, "true");
		} else if ("false".equalsIgnoreCase(objectReuse)) {
			LOG.debug("disable object reuse");
			streamEnv.getConfig().disableObjectReuse();
			log(ConfConstants.OBJECT_REUSE, "false");
		}

		String restartStrategyName = userParams.getProperty(ConfigConstants.RESTART_STRATEGY);
		if (restartStrategyName != null) {
			RestartStrategies.RestartStrategyConfiguration restart;
			switch (restartStrategyName) {
				case "off":
				case "disable":
					restart = RestartStrategies.noRestart();
					streamEnv.setRestartStrategy(restart);
					log(ConfigConstants.RESTART_STRATEGY, restart.getDescription());
					break;
				case "fixeddelay":
				case "fixed-delay":
					String maxAttemptsStr = userParams.getProperty(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
					int maxAttemps = ConfigConstants.DEFAULT_EXECUTION_RETRIES;
					if (maxAttemptsStr != null) {
						try {
							maxAttemps = Integer.parseInt(maxAttemptsStr);
						} catch (NumberFormatException nfe) {
							throw new Exception("Invalid config value for " +
												ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS + ": " + maxAttemptsStr +
												". Value must be a valid number.");
						}
					}

					String fixedDelayString = userParams.getProperty(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY.key(),
																	ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY.defaultValue());

					long fixedDelay;

					try {
						fixedDelay = Duration.apply(fixedDelayString).toMillis();
					} catch (NumberFormatException nfe) {
						throw new Exception("Invalid config value for " +
											ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY + ": " + fixedDelayString +
											". Value must be a valid duration (such as '100 milli' or '10 s')");
					}

					restart = RestartStrategies.fixedDelayRestart(maxAttemps, fixedDelay);
					streamEnv.setRestartStrategy(restart);
					log(ConfigConstants.RESTART_STRATEGY, restart.getDescription());
					break;
				case "failurerate":
				case "failure-rate":
					String maxFailuresPerIntervalStr = userParams.getProperty(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
					int maxFailuresPerInterval = 1;
					if (maxFailuresPerIntervalStr != null) {
						maxFailuresPerInterval = Integer.parseInt(maxFailuresPerIntervalStr);
					}

					String failuresIntervalString = userParams.getProperty(
						ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.apply(1, TimeUnit.MINUTES).toString()
					);

					String timeoutString = userParams.getProperty(AkkaOptions.WATCH_HEARTBEAT_INTERVAL.key(),
																AkkaOptions.WATCH_HEARTBEAT_INTERVAL.defaultValue());
					String delayString = userParams.getProperty(ConfigConstants.RESTART_STRATEGY_FAILURE_RATE_DELAY, timeoutString);

					Duration failuresInterval = Duration.apply(failuresIntervalString);
					Duration delay = Duration.apply(delayString);

					restart = RestartStrategies.failureRateRestart(maxFailuresPerInterval, Time.milliseconds(failuresInterval.toMillis()), Time.milliseconds(delay.toMillis()));
					streamEnv.setRestartStrategy(restart);
					log(ConfigConstants.RESTART_STRATEGY, restart.getDescription());
					break;
			}
		}
	}
}
