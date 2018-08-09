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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.GroupSet;
import org.apache.flink.runtime.state.InternalStateAccessTestBase;
import org.apache.flink.runtime.state.InternalStateBackend;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.util.AbstractID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Unit tests to validate that internal states can be correctly accessed in
 * {@link RocksDBInternalStateBackend}.
 */
public class RocksDBInternalStateAccessTest extends InternalStateAccessTestBase {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private DBOptions dbOptions;

	private ColumnFamilyOptions columnOptions;

	private boolean rocksDbInitialized = false;

	private static String rocksDBLoadPath;

	@BeforeClass
	public static void setupRocksLoadPath() throws IOException {
		rocksDBLoadPath = temporaryFolder.newFolder().getAbsolutePath();
	}

	@After
	public void disposeOptions() {
		if (dbOptions != null) {
			dbOptions.close();
		}
		if (columnOptions != null) {
			columnOptions.close();
		}
	}

	@Override
	protected InternalStateBackend createStateBackend(
		int numberOfGroups,
		GroupSet groups,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig) throws Exception {

		dbOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED_HIGH_MEM.createDBOptions().setCreateIfMissing(true);
		columnOptions = PredefinedOptions.FLASH_SSD_OPTIMIZED_HIGH_MEM.createColumnOptions();
		ensureRocksDBIsLoaded(rocksDBLoadPath);
		return new RocksDBInternalStateBackend(
			userClassLoader,
			temporaryFolder.newFolder().getAbsoluteFile(),
			dbOptions,
			columnOptions,
			numberOfGroups,
			groups,
			true,
			localRecoveryConfig,
			null);
	}

	private void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
		synchronized (RocksDBInternalStateBackend.class) {
			if (!rocksDbInitialized) {

				final File tempDirParent = new File(tempDirectory).getAbsoluteFile();

				Throwable lastException = null;
				for (int attempt = 1; attempt <= 3; attempt++) {
					try {
						// when multiple instances of this class and RocksDB exist in different
						// class loaders, then we can see the following exception:
						// "java.lang.UnsatisfiedLinkError: Native Library /path/to/temp/dir/librocksdbjni-linux64.so
						// already loaded in another class loader"

						// to avoid that, we need to add a random element to the library file path
						// (I know, seems like an unnecessary hack, since the JVM obviously can handle multiple
						//  instances of the same JNI library being loaded in different class loaders, but
						//  apparently not when coming from the same file path, so there we go)

						final File rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());

						// make sure the temp path exists
						// noinspection ResultOfMethodCallIgnored
						rocksLibFolder.mkdirs();

						// explicitly load the JNI dependency if it has not been loaded before
						NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());

						// this initialization here should validate that the loading succeeded
						RocksDB.loadLibrary();

						// seems to have worked
						rocksDbInitialized = true;
						return;
					}
					catch (Throwable t) {
						lastException = t;

						// try to force RocksDB to attempt reloading the library
						try {
							resetRocksDBLoadedFlag();
						} catch (Throwable tt) {
						}
					}
				}

				throw new IOException("Could not load the native RocksDB library", lastException);
			}
		}
	}

	@VisibleForTesting
	static void resetRocksDBLoadedFlag() throws Exception {
		final Field initField = org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}
}
