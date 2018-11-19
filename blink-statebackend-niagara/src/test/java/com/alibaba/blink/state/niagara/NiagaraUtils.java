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

import com.alibaba.niagara.NiagaraJNI;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;

/**
 * Utils used for NiagaraStateBackend's tests.
 */
public class NiagaraUtils {

	public static void ensureNiagaraIsLoaded(String tempDirectory){
		final File tempDirParent = new File(tempDirectory).getAbsoluteFile();

		Throwable lastException = null;
		for (int attempt = 1; attempt <= 3; attempt++) {
			try {
				File tempDirFile = new File(tempDirParent, "niagara-library-" + UUID.randomUUID().toString());

				// make sure the temp path exists
				// noinspection ResultOfMethodCallIgnored
				tempDirFile.mkdirs();
				tempDirFile.deleteOnExit();

				// this initialization here should validate that the loading succeeded
				NiagaraJNI.loadLibrary(tempDirFile.getAbsolutePath());

				return;
			} catch (Throwable t) {
				lastException = t;

				// try to force Niagara to attempt reloading the library
				try {
					resetNiagaraLoadedFlag();
				} catch (Throwable tt) {
				}
			}
		}

		throw new RuntimeException("Could not load the native Niagara library", lastException);
	}

	public static void resetNiagaraLoadedFlag() throws Exception {
		final Field initField = com.alibaba.niagara.NativeLibraryLoader.class.getDeclaredField("initialized");
		initField.setAccessible(true);
		initField.setBoolean(null, false);
	}
}
