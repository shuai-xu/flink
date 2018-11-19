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

package com.alibaba.blink.state.niagara;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.runtime.state.GroupRange;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.util.Preconditions;

import com.alibaba.niagara.NiagaraException;
import com.alibaba.niagara.NiagaraIterator;
import com.alibaba.niagara.NiagaraJNI;
import com.alibaba.niagara.ReadOptions;
import com.alibaba.niagara.TabletDescriptor;
import com.alibaba.niagara.TabletOptions;
import com.alibaba.niagara.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TabletInstance in Niagara.
 */
public class NiagaraTabletInstance implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(NiagaraTabletInstance.class);

	static final String SST_FILE_SUFFIX = ".sst";

	static final byte[] MERGE_SEPARATOR_BYTE = ",".getBytes(ConfigConstants.DEFAULT_CHARSET);

	static final int MERGE_SEPARATOR = MERGE_SEPARATOR_BYTE[0];

	/** The magic (maximum) sequence id for Niagara which will let it inherit from latest checkpoint under candidate tablet. */
	private static final long MAGIC_SEQUENCE_ID = 72057594037927935L;

	/**
	 * Our Niagara database, The different k/v state will share the same niagara instance.
	 * NOTE: Niagara is implemented a singleton.
	 */
	private final NiagaraJNI db;

	/**
	 * Our Niagara tablet, The k/v states of the KeyGroupRange are write to this tablet.
	 */
	private long tabletHandle;

	/** The options used in Tablet instance. */
	private TabletOptions tabletOptions;
	private WriteOptions writeOptions;
	private ReadOptions readOptions;

	/**
	 *  Create Niagara tablet instance with given parameters.
	 *
	 * @param db The niagara JNI.
	 * @param tabletPath Tablet path for this Niagara tablet.
	 * @param tabletName Tablet name for this Niagara tablet, default the same as tablet path.
	 * @param config The configuration used to init tablet.
	 * @throws NiagaraException
	 */
	NiagaraTabletInstance(
		NiagaraJNI db,
		File tabletPath,
		String tabletName,
		NiagaraConfiguration config) throws NiagaraException {

		this.db = db;

		this.tabletOptions = initTabletOptions(config);
		TabletDescriptor tableDescriptor = new TabletDescriptor(tabletPath.getPath(), tabletName, tabletOptions);

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
		readOptions = new ReadOptions();
		tabletHandle = this.db.createTablet(tableDescriptor);
	}

	/**
	 * Create Niagara tablet inheriting from local restored tablets.
	 */
	NiagaraTabletInstance(
		NiagaraJNI db,
		File tabletPath,
		String tabletName,
		NiagaraConfiguration config,
		GroupRange groupRange,
		List<Path> restoredTabletPaths
	) throws IOException, NiagaraException {

		this(db,
			tabletPath,
			tabletName,
			config,
			groupRange,
			restoredTabletPaths,
			Collections.nCopies(restoredTabletPaths.size(), MAGIC_SEQUENCE_ID));
	}

	/**
	 * Create Niagara tablet inheriting from local restored tablets with snapshot id each other.
	 *
	 * @param db The Niagara JNI.
	 * @param tabletPath The tablet Path for new created tablet.
	 * @param tabletName The tablet name for new created tablet.
	 * @param config The configuration used to init tablet.
	 * @param groupRange The group range used to inherit from several candidate tablets.
	 * @param restoredTabletPaths The restored tablet paths, each of them represent one restored tablet.
	 * @param snapshotIds The snapshot ids, one-to-one corresponded to the restored paths
	 */
	NiagaraTabletInstance(
		NiagaraJNI db,
		File tabletPath,
		String tabletName,
		NiagaraConfiguration config,
		GroupRange groupRange,
		List<Path> restoredTabletPaths,
		List<Long> snapshotIds) throws NiagaraException, IOException {

		Preconditions.checkState(!restoredTabletPaths.isEmpty());
		Preconditions.checkArgument(restoredTabletPaths.size() == snapshotIds.size(),
			"The snapshots ids must be one-to-one corresponded to the restored tablet paths");

		FileSystem fileSystem = restoredTabletPaths.get(0).getFileSystem();
		for (Path restoredTabletPath : restoredTabletPaths) {
			Preconditions.checkState(fileSystem.exists(restoredTabletPath), "The candidate restored tablet path: " + restoredTabletPath + " no existed.");
		}

		this.db = db;

		this.tabletOptions = initTabletOptions(config);
		TabletDescriptor tableDescriptor = new TabletDescriptor(tabletPath.getPath() + "/", tabletName, tabletOptions);

		Map<String, Long> checkpointHandles = new HashMap<>();
		for (int i = 0; i < restoredTabletPaths.size(); i++) {
			checkpointHandles.put(restoredTabletPaths.get(i).toUri().toString() + "/", snapshotIds.get(i));
		}

		StringBuilder sb = new StringBuilder();
		checkpointHandles.forEach((k, v) -> sb.append("[" + k + ", snapshot id: " + v + "] "));
		LOG.info("Restoring Niagara tablet at {} from handles: {} of range {}.", tableDescriptor.tabletDir(), sb.toString(), groupRange);

		tabletHandle =  this.db.restoreCheckpoint(
			tableDescriptor,
			serializeGroupPrefix(groupRange.getStartGroup()),
			serializeGroupPrefix(groupRange.getEndGroup()),
			checkpointHandles);

		writeOptions = new WriteOptions();
		writeOptions.setDisableWAL(true);
		readOptions = new ReadOptions();
	}

	@Override
	public void close() {
		try {
			db.dropTablet(tabletHandle);
		} catch (NiagaraException e) {
			LOG.warn("Fail to drop tablet.", e);
		}
		if (readOptions != null) {
			readOptions.close();
		}
		if (writeOptions != null) {
			writeOptions.close();
		}
		if (tabletOptions != null) {
			tabletOptions.close();
		}
	}

	byte[] get(byte[] keyBytes) {
		try {
			return db.get(tabletHandle, readOptions, keyBytes);
		} catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	Map<byte[], byte[]> multiGet(List<byte[]> keyBytesList) {
		try {
			return db.multiGet(tabletHandle, readOptions, keyBytesList);
		} catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	void put(byte[] keyBytes, byte[] valueBytes) {
		try {
			db.put(tabletHandle, writeOptions, keyBytes, valueBytes);
		} catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	void multiPut(Map<byte[], byte[]> keyValueBytesMap) {
		try {
			db.multiPut(tabletHandle, writeOptions, keyValueBytesMap);
		} catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	void delete(byte[] keyBytes) {
		try {
			db.delete(tabletHandle, writeOptions, keyBytes);
		}  catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	void merge(byte[] keyBytes, byte[] partialValueBytes) {
		try {
			db.merge(tabletHandle, writeOptions, keyBytes, partialValueBytes);
		} catch (NiagaraException e) {
			throw new StateAccessException(e);
		}
	}

	NiagaraIterator iterator() {
		return db.newIterator(tabletHandle);
	}

	NiagaraIterator iterator(ReadOptions readOptions) {
		return db.newIterator(tabletHandle, readOptions);
	}

	NiagaraIterator reverseIterator() {
		return db.newReverseIterator(tabletHandle);
	}

	void snapshot(String checkpointPath) throws NiagaraException{
		db.createCheckpoint(tabletHandle, checkpointPath);
	}

	public static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
		Preconditions.checkArgument(bytes != null);
		Preconditions.checkArgument(prefixBytes != null);

		if (bytes.length < prefixBytes.length) {
			return false;
		}

		for (int i = 0; i < prefixBytes.length; ++i) {
			if (bytes[i] != prefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	public static int compare(byte[] leftBytes, byte[] rightBytes) {
		Preconditions.checkArgument(leftBytes != null);
		Preconditions.checkArgument(rightBytes != null);

		int commonLength = Math.min(leftBytes.length, rightBytes.length);
		for (int i = 0; i < commonLength; ++i) {
			int leftByte = leftBytes[i] & 0xFF;
			int rightByte = rightBytes[i] & 0xFF;

			if (leftByte > rightByte) {
				return 1;
			} else if (leftByte < rightByte) {
				return -1;
			}
		}
		return (leftBytes.length - rightBytes.length);
	}

	//--------------------------------------------------------------------------

	private TabletOptions initTabletOptions(NiagaraConfiguration config) throws NiagaraException {
		TabletOptions tabletOptions = new TabletOptions();

		tabletOptions.setTtlMillisecond(config.getTtl());

		// memory optimization for performance
		tabletOptions.setMemtableSizeInBytes(config.getMemtableSize());
		tabletOptions.setBlockCacheSizeInBytes(config.getBlockCacheSize());

		// To consistent with the binary format prescribed by Niagara.
		// Use StringAppendOperator as merge operator and "," as the default separator
		tabletOptions.setMergeOperator("StringAppendOperator");
		tabletOptions.setMergeOperatorOptions(MERGE_SEPARATOR_BYTE);
		return tabletOptions;
	}

	private byte[] serializeGroupPrefix(int group) {
		ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();

		NiagaraInternalState.writeInt(outputStream, group);

		return outputStream.toByteArray();
	}
}
