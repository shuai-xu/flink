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

package com.alibaba.blink.launcher.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.OptionsFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.concurrent.duration.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.MERGE_OPERATOR_NAME;

/**
 * Configuration for RocksDB state-backend.
 */
public class RocksDBConfiguration implements OptionsFactory {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBConfiguration.class);

	private static final long ROCKSDB_WRITEBUFFER_SIZE_MIN = 8 * SizeUnit.MB;
	private static final long ROCKSDB_WRITEBUFFER_SIZE_MAX = 32 * SizeUnit.MB;
	private static final long ROCKSDB_BLOCK_CACHE_SIZE_MIN = 64 * SizeUnit.MB;
	private static final long ROCKSDB_BLOCK_CACHE_SIZE_MAX = 1 * SizeUnit.GB;

	public static final ConfigOption<String> DATA_PATHS =
		key("state.backend.rocksdb.datapath")
			.noDefaultValue()
			.withDescription("The directories where the rocksDB locates, could be a string with ',' as delimiter.");

	public static final ConfigOption<String> TTL =
		key("state.backend.rocksdb.ttl")
			.defaultValue("3 d")
			.withDescription("The time in seconds to live for data in RocksDB, the default value is 3 days.");

	public static final ConfigOption<String> BLOCK_SIZE =
		key("state.backend.rocksdb.block.blocksize")
			.defaultValue("4 kb")
			.withDescription("The approximate size (in bytes) of user data packed per block." +
				" Note that the block size specified here corresponds to uncompressed data," +
				" and the actual size of the unit read from disk may be smaller if compression is enabled." +
				" The default value is 4kb.");

	public static final ConfigOption<String> BLOCK_CACHE_SIZE =
		key("state.backend.rocksdb.block.cachesize")
			.defaultValue("512 mb")
			.withDescription("The amount of the cache (in megabytes) for data blocks in RocksDB." +
				" The default value is 512mb. And the max allowed cache-size is 1gb while the min is 64mb.");

	public static final ConfigOption<Boolean> CACHE_INDEX_FILTER =
		key("state.backend.rocksdb.block.cacheindexandfilter")
			.defaultValue(true)
			.withDescription("Indicating whether we'd put index/filter blocks to the block cache." +
				" The default value is true.");

	public static final ConfigOption<Integer> THREAD_FLUSHES =
		key("state.backend.rocksdb.thread.flushes")
			.defaultValue(2)
			.withDescription("The maximum number of concurrent background flush jobs." +
				" The default value is 2.");

	public static final ConfigOption<Integer> THREAD_COMPACTIONS =
		key("state.backend.rocksdb.thread.compactions")
			.defaultValue(2)
			.withDescription("The maximum number of concurrent background compaction jobs." +
				" The default value is 2");

	public static final ConfigOption<String> WRITE_BUFFER_SIZE =
		key("state.backend.rocksdb.writebuffer.size")
			.defaultValue("32 mb")
			.withDescription("The amount of data built up in memory (backed by an unsorted log on disk)" +
				" before converting to a sorted on-disk files. The default value is 32mb." +
				" And the max allowed size is 32mb while the min size is 8mb.");

	public static final ConfigOption<Integer> WRITE_BUFFER_NUMBER =
		key("state.backend.rocksdb.writebuffer.count")
			.defaultValue(4)
			.withDescription("Tne maximum number of write buffers that are built up in memory." +
				" The default value is 4.");

	public static final ConfigOption<Integer> LEVEL_NUMBER =
		key("state.backend.rocksdb.compaction.levels")
			.defaultValue(4)
			.withDescription("The number of levels for rocksDB when level-style compaction is used." +
				" The default value is 4.");

	public static final ConfigOption<Integer> LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER =
		key("state.backend.rocksdb.compaction.level0files")
			.defaultValue(4)
			.withDescription("The number of files to trigger level-0 compaction." +
				" The default value is 4.");

	public static final ConfigOption<String> LEVEL1_FILE_TARGET_SIZE =
		key("state.backend.rocksdb.compaction.level1targetsize")
			.defaultValue("64 mb")
			.withDescription("The target file size for compaction, which determines a level-1 file size." +
				" The default value is 64mb.");

	public static final ConfigOption<String> LEVEL1_MAX_SIZE =
		key("state.backend.rocksdb.compaction.level1maxsize")
			.defaultValue("512 mb")
			.withDescription("The upper-bound of the total size of level-1 files.");

	public static final ConfigOption<String> COMPRESSION_TYPE =
		key("state.backend.rocksdb.compress")
			.defaultValue(CompressionType.SNAPPY_COMPRESSION.getLibraryName())
			.withDescription("The specified compression algorithm used to compress blocks." +
				" The default value is 'snappy' which gives lightweight but fast compression.");

	public static final ConfigOption<Boolean> INPLACE_UPDATE =
		key("state.backend.rocksdb.inplaceupdate")
			.defaultValue(false)
			.withDescription("True if thread-safe inplace updates are allowed." +
				" The default value is false.");

	public static final ConfigOption<Boolean> OPTIMIZE_HIT =
		key("state.backend.rocksdb.optimize-filter-hits")
			.defaultValue(false)
			.withDescription("Optimize filter hits, allows rocksDB to not store filters for the last level i.e\n" +
				" the largest level which contains data of the LSM store.");

	public static final ConfigOption<String> INDEX_TYPE =
		key("state.backend.rocksdb.index-type")
			.defaultValue(IndexType.kBinarySearch.name())
			.withDescription("IndexType used in conjunction with BlockBasedTable, use partitioned index as default.");

	public static final ConfigOption<Boolean> USE_LOG_DIR =
		key("state.backend.rocksdb.use-log-dir")
			.defaultValue(true)
			.withDescription("This specifies whether we set the LOG dir for rocksDB. " +
				"If true, the log files will try to locate in the parent folder of environment property 'log.file', " +
				"If false or environment property 'log.file' is not set, the log files will be in the same dir as data.");

	public static final ConfigOption<String> MAX_LOG_SIZE =
		key("state.backend.rocksdb.log.max-size")
			.defaultValue("128mb")
			.withDescription("Specifies the maximum size of a info log file. If the current log file is larger than it," +
				" a new info log file will be created. If 0, all logs will be written to one log file.");

	public static final ConfigOption<Integer> KEEP_LOG_FILE_NUM =
		key("state.backend.rocksdb.log.keep-num")
			.defaultValue(50)
			.withDescription("Specifies the maximum number of info log files to be kept.");

	public static final ConfigOption<Integer> CHECKPOINT_RESTORE_THREAD_NUMS =
		key("state.backend.rocksdb.checkpoint-restore-thread-nums")
		.defaultValue(5)
		.withDescription("The thread number used for restoring file from DFS.");

	private final Map<String, String> entries = initConfigurationWithDefaultValue();

	private static Map<String, String> initConfigurationWithDefaultValue() {
		ConfigOption[] entries = new ConfigOption[]{
			DATA_PATHS,
			TTL,
			BLOCK_SIZE,
			BLOCK_CACHE_SIZE,
			CACHE_INDEX_FILTER,
			THREAD_FLUSHES,
			THREAD_COMPACTIONS,
			WRITE_BUFFER_SIZE,
			WRITE_BUFFER_NUMBER,
			LEVEL_NUMBER,
			LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER,
			LEVEL1_FILE_TARGET_SIZE,
			LEVEL1_MAX_SIZE,
			COMPRESSION_TYPE,
			INPLACE_UPDATE,
			OPTIMIZE_HIT,
			INDEX_TYPE,
			USE_LOG_DIR,
			MAX_LOG_SIZE,
			KEEP_LOG_FILE_NUM,
			CHECKPOINT_RESTORE_THREAD_NUMS
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

	/**
	 * Create rocksDB configuration with default key-values.
	 */
	public RocksDBConfiguration() {
		// Nothing to do
	}

	/**
	 * Extract rocksDB configuration from given configuration set.
	 */
	public RocksDBConfiguration(Configuration configuration) {
		for (Map.Entry<String, String> entry : entries.entrySet()) {
			String key = entry.getKey();
			String newValue = configuration.getString(key, null);

			if (newValue != null) {
				newValue = checkArgumentValid(key, newValue);
				entry.setValue(newValue);
			}
		}
	}

	/**
	 * Convert {@link RocksDBConfiguration} to Blink's {@link Configuration}.
	 */
	public Configuration toBlinkConf() {
		Configuration configuration = new Configuration();
		for (Map.Entry<String, String> entry : entries.entrySet()) {
			String value = entry.getValue();
			if (value != null) {
				configuration.setString(entry.getKey(), value);
			}
		}
		return configuration;
	}

	//--------------------------------------------------------------------------
	// The data paths used in rocksDB
	//--------------------------------------------------------------------------

	public List<Path> getDataPaths() {
		List<Path> dataPaths = new ArrayList<>();

		String str = getInternal(DATA_PATHS.key());
		if (str != null) {
			String[] dataUrls = str.split(",");
			for (String dataUrl : dataUrls) {
				if (!dataUrl.isEmpty()) {
					dataPaths.add(new Path(dataUrl));
				}
			}
		}

		return dataPaths;
	}

	public RocksDBConfiguration setDataPaths(String str) {
		validateDataPaths(str);

		setInternal(DATA_PATHS.key(), str);
		return this;
	}

	private void validateDataPaths(String str) {
		String[] dataPathStrs = str.split(",");
		for (String dataPathStr : dataPathStrs) {
			if (!dataPathStr.isEmpty()) {
				Path dataPath = new Path(dataPathStr);
				String scheme = dataPath.toUri().getScheme();

				if (scheme != null && !scheme.equalsIgnoreCase("file")) {
					throw new IllegalArgumentException("Path " + dataPathStr + " has a non local scheme.");
				}
			}
		}
	}

	//--------------------------------------------------------------------------
	// The time to live for the data in rocksDB
	//--------------------------------------------------------------------------

	public int getTtl() {
		long ttlSeconds = Duration.apply(getInternal(TTL.key())).toSeconds();
		return ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
	}

	public RocksDBConfiguration setTtl(String ttl) {
		setInternal(TTL.key(), ttl);
		return this;
	}

	//--------------------------------------------------------------------------
	// Approximate size of user data packed per block. Note that the block size
	// specified here corresponds to uncompressed data. The actual size of the
	// unit read from disk may be smaller if compression is enabled
	//--------------------------------------------------------------------------

	public long getBlockSize() {
		return MemorySize.parseBytes(getInternal(BLOCK_SIZE.key()));
	}

	public RocksDBConfiguration setBlockSize(String blockSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blockSize) > 0);
		setInternal(BLOCK_SIZE.key(), blockSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// The amount of the cache for data blocks in RocksDB
	//--------------------------------------------------------------------------

	public long getBlockCacheSize() {
		return MemorySize.parseBytes(getInternal(BLOCK_CACHE_SIZE.key()));
	}

	public RocksDBConfiguration setBlockCacheSize(String blockCacheSize) {
		long blockCacheSizeBytes = MemorySize.parseBytes(blockCacheSize);
		if (blockCacheSizeBytes > ROCKSDB_BLOCK_CACHE_SIZE_MAX || blockCacheSizeBytes < ROCKSDB_BLOCK_CACHE_SIZE_MIN) {
			LOG.warn("Invalid configuration for block cache size. The value will still remain as previous value(" + getBlockCacheSize() + " bytes).");
		} else {
			setInternal(BLOCK_CACHE_SIZE.key(), blockCacheSize);
		}

		return this;
	}

	//--------------------------------------------------------------------------
	// Cache index and filter blocks in block cache
	//--------------------------------------------------------------------------

	public boolean getCacheIndexAndFilterBlocks() {
		return getInternal(CACHE_INDEX_FILTER.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBConfiguration setCacheIndexAndFilterBlocks(boolean value) {
		setInternal(CACHE_INDEX_FILTER.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// Maximum number of concurrent background memtable flush jobs
	//--------------------------------------------------------------------------

	public int getMaxBackgroundFlushes() {
		return Integer.parseInt(getInternal(THREAD_FLUSHES.key()));
	}

	public RocksDBConfiguration setMaxBackgroundFlushes(int flushThreadCount) {
		Preconditions.checkArgument(flushThreadCount > 0);
		setInternal(THREAD_FLUSHES.key(), Integer.toString(flushThreadCount));
		return this;
	}

	//--------------------------------------------------------------------------
	// Amount of data to build up in memory (backed by an unsorted log on disk)
	// before converting to a sorted on-disk file. Larger values increase
	// performance, especially during bulk loads.
	//--------------------------------------------------------------------------

	public long getWriteBufferSize() {
		return MemorySize.parseBytes(getInternal(WRITE_BUFFER_SIZE.key()));
	}

	public RocksDBConfiguration setWriteBufferSize(String writeBufferSize) {
		long writeBufferSizeBytes = MemorySize.parseBytes(writeBufferSize);
		if (writeBufferSizeBytes > ROCKSDB_WRITEBUFFER_SIZE_MAX || writeBufferSizeBytes < ROCKSDB_WRITEBUFFER_SIZE_MIN) {
			LOG.warn("Invalid configuration for write buffer size. The value will still remain as previous value(" + getWriteBufferSize() + " bytes).");
		} else {
			setInternal(WRITE_BUFFER_SIZE.key(), writeBufferSize);
		}
		return this;
	}

	//--------------------------------------------------------------------------
	// The maximum number of write buffers that are built up in memory.
	//--------------------------------------------------------------------------

	public int getMaxWriteBufferNumber() {
		return Integer.parseInt(getInternal(WRITE_BUFFER_NUMBER.key()));
	}

	public RocksDBConfiguration setMaxWriteBufferNumber(int writeBufferNumber) {
		Preconditions.checkArgument(writeBufferNumber > 0);
		setInternal(WRITE_BUFFER_NUMBER.key(), Integer.toString(writeBufferNumber));
		return this;
	}

	//--------------------------------------------------------------------------
	// Whether optimize filters for hits
	//--------------------------------------------------------------------------

	public boolean getOptimizeFiltersForHits() {
		return getInternal(OPTIMIZE_HIT.key()).compareToIgnoreCase("false") != 0;
	}

	public RocksDBConfiguration setOptimizeFiltersForHits(boolean value) {
		setInternal(OPTIMIZE_HIT.key(), value ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// Maximum number of concurrent background compaction jobs.
	//--------------------------------------------------------------------------

	public int getMaxBackgroundCompactions() {
		return Integer.parseInt(getInternal(THREAD_COMPACTIONS.key()));
	}

	public RocksDBConfiguration setMaxBackgroundCompactions(int compactionThreadCount) {
		Preconditions.checkArgument(compactionThreadCount > 0);
		setInternal(THREAD_COMPACTIONS.key(), Integer.toString(compactionThreadCount));
		return this;
	}

	//--------------------------------------------------------------------------
	// Number of levels for this database used in level-style compaction.
	//--------------------------------------------------------------------------

	public int getNumLevels() {
		return Integer.parseInt(getInternal(LEVEL_NUMBER.key()));
	}

	public RocksDBConfiguration setNumLevels(int numLevels) {
		Preconditions.checkArgument(numLevels > 0);
		setInternal(LEVEL_NUMBER.key(), Integer.toString(numLevels));
		return this;
	}

	//--------------------------------------------------------------------------
	// Number of files to trigger level-0 compaction
	//--------------------------------------------------------------------------

	public int getLevel0FileNumCompactionTrigger() {
		return Integer.parseInt(getInternal(LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER.key()));
	}

	public RocksDBConfiguration setLevel0FileNumCompactionTrigger(int level0FileCount) {
		Preconditions.checkArgument(level0FileCount >= 0);
		setInternal(LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER.key(), Integer.toString(level0FileCount));
		return this;
	}

	//--------------------------------------------------------------------------
	// The target file size for compaction, i.e., the per-file size for level-1
	//--------------------------------------------------------------------------

	public long getLevel1FileTargetSize() {
		return MemorySize.parseBytes(getInternal(LEVEL1_FILE_TARGET_SIZE.key()));
	}

	public RocksDBConfiguration setLevel1FileTargetSize(String level1FileTargetSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(level1FileTargetSize) > 0);
		setInternal(LEVEL1_FILE_TARGET_SIZE.key(), level1FileTargetSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// Maximum total data size for a level, i.e., the max total size for level-1
	//--------------------------------------------------------------------------

	public long getLevel1MaxSize() {
		return MemorySize.parseBytes(getInternal(LEVEL1_MAX_SIZE.key()));
	}

	public RocksDBConfiguration setLevel1MaxSize(String level1MaxSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(level1MaxSize) > 0);
		setInternal(LEVEL1_MAX_SIZE.key(), level1MaxSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// Sets the type of compression.
	//--------------------------------------------------------------------------

	public CompressionType getCompressionType() {
		return CompressionType.getCompressionType(getInternal(COMPRESSION_TYPE.key()));
	}

	public RocksDBConfiguration setCompressionType(CompressionType compressionType) {
		setInternal(COMPRESSION_TYPE.key(), compressionType.getLibraryName());
		return this;
	}

	//--------------------------------------------------------------------------
	// Sets the type of index.
	//--------------------------------------------------------------------------

	public IndexType getIndexType() {
		return IndexType.valueOf(getInternal(INDEX_TYPE.key()));
	}

	public RocksDBConfiguration setIndexType(IndexType indexType) {
		setInternal(INDEX_TYPE.key(), indexType.name());
		return this;
	}

	//--------------------------------------------------------------------------
	// Allows thread-safe inplace updates. If this is true, there is no way to
	// achieve point-in-time consistency using snapshot or iterator (assuming
	// concurrent updates). Hence iterator and multi-get will return results
	// which are not consistent as of any point-in-time.
	//--------------------------------------------------------------------------

	public boolean getInplaceUpdate() {
		return getInternal(INPLACE_UPDATE.key()).compareTo("true") == 0;
	}

	public RocksDBConfiguration setInplaceUpdate(boolean inplaceUpdate) {
		setInternal(INPLACE_UPDATE.key(), inplaceUpdate ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// Allows rocksDB log located in the parent folder of environment property
	// 'log.file' if this property is set up.
	//--------------------------------------------------------------------------

	public boolean isUseLogDir() {
		return getInternal(USE_LOG_DIR.key()).compareToIgnoreCase("true") == 0;
	}

	public RocksDBConfiguration setUseLogDir(boolean useLogDir) {
		setInternal(USE_LOG_DIR.key(), useLogDir ? "true" : "false");
		return this;
	}

	//--------------------------------------------------------------------------
	// The maximum size of a info log file
	//--------------------------------------------------------------------------

	public long getMaxLogFileSize() {
		return MemorySize.parseBytes(getInternal(MAX_LOG_SIZE.key()));
	}

	public RocksDBConfiguration setMaxLogFileSize(String maxLogFileSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(maxLogFileSize) >= 0);
		setInternal(MAX_LOG_SIZE.key(), maxLogFileSize);
		return this;
	}

	//--------------------------------------------------------------------------
	// Specifies the maximum number of info log files to be kept.
	//--------------------------------------------------------------------------

	public int getKeepLogFileNum() {
		return Integer.parseInt(getInternal(KEEP_LOG_FILE_NUM.key()));
	}

	public RocksDBConfiguration setKeepLogFileNum(int keepLogFileNum) {
		Preconditions.checkArgument(keepLogFileNum > 0);
		setInternal(KEEP_LOG_FILE_NUM.key(), Integer.toString(keepLogFileNum));
		return this;
	}

	// Sets the thread number used for restoring file from DFS.
	//--------------------------------------------------------------------------

	public int getCheckpointRestoreThreadNums() {
		return Integer.parseInt(getInternal(CHECKPOINT_RESTORE_THREAD_NUMS.key()));
	}

	public RocksDBConfiguration setCheckpointRestoreThreadNums(int threadNums) {
		Preconditions.checkArgument(threadNums > 0 && threadNums <= 30, "Thread number used for restoring should between 1 and 30");
		setInternal(CHECKPOINT_RESTORE_THREAD_NUMS.key(), Integer.toString(threadNums));
		return this;
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

	private static Set<String> nonNegativeIntConfigSet = new HashSet<>(Arrays.asList(
		THREAD_FLUSHES.key(),
		THREAD_COMPACTIONS.key(),
		WRITE_BUFFER_NUMBER.key(),
		LEVEL_NUMBER.key(),
		LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER.key()));

	private static Set<String> sizeConfigSet = new HashSet<>(Arrays.asList(
		BLOCK_SIZE.key(),
		LEVEL1_FILE_TARGET_SIZE.key(),
		LEVEL1_MAX_SIZE.key()));

	private static Set<String> booleanConfigSet = new HashSet<>(Arrays.asList(CACHE_INDEX_FILTER.key(), INPLACE_UPDATE.key(), USE_LOG_DIR.key()));

	private static Set<String> compressionTypeSet = Arrays.stream(CompressionType.values()).map(CompressionType::getLibraryName).collect(Collectors.toSet());

	/**
	 * Helper method to check whether the (key,value) is valid through given configuration and returns the formatted value.
	 *
	 * @param key The configuration key which is predefined in {@link RocksDBConfiguration}.
	 * @param value The value within given configuration.
	 */
	private String checkArgumentValid(String key, String value) {
		if (nonNegativeIntConfigSet.contains(key)) {

			if (key.equals(LEVEL0_FILE_NUMBER_COMPACTION_TRIGGER.key())) {
				Preconditions.checkArgument(Integer.parseInt(value) >= 0,
					"Configured value for key: " + key + " must be non-negative.");
			} else {
				Preconditions.checkArgument(Integer.parseInt(value) > 0,
					"Configured value for key: " + key + " must be larger than 0.");
			}
		} else if (sizeConfigSet.contains(key)) {

			Preconditions.checkArgument(MemorySize.parseBytes(value) > 0,
				"Configured size for key" + key + " must be larger than 0.");
		} else if (booleanConfigSet.contains(key)) {

			Preconditions.checkArgument("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
				"The configured boolean value: " + value + " for key: " + key + " is illegal.");
		} else if (key.equals(BLOCK_CACHE_SIZE.key())) {
			long blockCacheSizeBytes = MemorySize.parseBytes(value);

			Preconditions.checkArgument(blockCacheSizeBytes <= ROCKSDB_BLOCK_CACHE_SIZE_MAX && blockCacheSizeBytes >= ROCKSDB_BLOCK_CACHE_SIZE_MIN,
				"Configured cache size for " + key + " should be in [" + ROCKSDB_BLOCK_CACHE_SIZE_MIN + ", " + ROCKSDB_BLOCK_CACHE_SIZE_MAX + "] bytes range.");
		} else if (key.equals(WRITE_BUFFER_SIZE.key())) {
			long writeBufferSizeBytes = MemorySize.parseBytes(value);

			Preconditions.checkArgument(writeBufferSizeBytes <= ROCKSDB_WRITEBUFFER_SIZE_MAX && writeBufferSizeBytes >= ROCKSDB_WRITEBUFFER_SIZE_MIN,
				"Configured write buffer size for " + key + " should be in [" + ROCKSDB_WRITEBUFFER_SIZE_MIN + ", " + ROCKSDB_WRITEBUFFER_SIZE_MAX + "] bytes range.");
		} else if (key.equals(COMPRESSION_TYPE.key())) {
			value = value.toLowerCase();
			Preconditions.checkArgument(compressionTypeSet.contains(value),
				"Compression type: " + value + " is not recognized with legal types: " + compressionTypeSet.stream().collect(Collectors.joining(", ")));
		} else if (key.equals(DATA_PATHS.key())) {
			validateDataPaths(value);
		} else if (key.equals(CHECKPOINT_RESTORE_THREAD_NUMS.key())) {
			Integer intValue = Integer.valueOf(value);
			Preconditions.checkArgument(intValue > 0 && intValue <= 30, "Thread number used for restoring should between 1 and 30.");
		}
		return value;
	}

	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder("RocksDBConfiguration{");

		for (Map.Entry<String, String> entry : entries.entrySet()) {
			ret.append("\n\t").append(entry.getKey()).append(": ").append(entry.getValue());
		}
		ret.append("}");

		return ret.toString();
	}

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions) {
		return currentOptions
				.setUseFsync(false)
				.setMaxOpenFiles(-1)
				.setMaxBackgroundCompactions(getMaxBackgroundCompactions())
				.setMaxBackgroundFlushes(getMaxBackgroundFlushes())
				.setStatsDumpPeriodSec(60)
				.setCreateIfMissing(true);
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
		return currentOptions
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevelCompactionDynamicLevelBytes(true)
				.setTargetFileSizeBase(getLevel1FileTargetSize())
				.setMaxBytesForLevelBase(getLevel1MaxSize())
				.setWriteBufferSize(getWriteBufferSize())
				.setMaxWriteBufferNumber(getMaxWriteBufferNumber())
				.setOptimizeFiltersForHits(getOptimizeFiltersForHits())
				.setLevelZeroFileNumCompactionTrigger(getLevel0FileNumCompactionTrigger())
				.setMergeOperatorName(MERGE_OPERATOR_NAME)
				.setTableFormatConfig(
						new BlockBasedTableConfig()
								.setFilter(new BloomFilter(10, false))
								.setIndexType(getIndexType())
								.setCacheIndexAndFilterBlocks(getCacheIndexAndFilterBlocks())
								.setBlockCacheSize(getBlockCacheSize())
								.setBlockSize(getBlockSize())
				);
	}
}
