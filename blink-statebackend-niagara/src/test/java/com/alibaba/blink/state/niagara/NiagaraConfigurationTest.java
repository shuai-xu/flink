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

package com.alibaba.blink.state.niagara;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Configuration tests in Niagara.
 */
public class NiagaraConfigurationTest {

	@Test
	public void testTimeConfiguration() {
		NiagaraConfiguration configuration = new NiagaraConfiguration();

		HashMap<String, Long> configValues = new HashMap<String, Long>() {{
			put("2s", TimeUnit.SECONDS.toMillis(2));
			put("2min", TimeUnit.MINUTES.toMillis(2));
			put("2h", TimeUnit.HOURS.toMillis(2));
			put("2d", TimeUnit.DAYS.toMillis(2));
		}};

		for (Map.Entry<String, Long> configEntry : configValues.entrySet()) {
			configuration.set(NiagaraConfiguration.TTL.key(), configEntry.getKey());
			assertEquals(configuration.getTtl(), configEntry.getValue().longValue());
			configuration.setTtl(Duration.apply(configEntry.getValue() * 2, TimeUnit.MILLISECONDS).toString());
			assertEquals(configuration.getTtl(), configEntry.getValue() * 2);
		}
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testTimeConfigurationWithErrorFormat() {
		NiagaraConfiguration configuration = new NiagaraConfiguration();

		configuration.set(NiagaraConfiguration.TTL.key(), "3213hd");
		configuration.getTtl();
	}

	@Test
	public void testSizeConfiguration() {
		NiagaraConfiguration configuration = new NiagaraConfiguration();

		HashMap<String, Long> configValues = new HashMap<String, Long>() {{
			put("2", 2L);
			put("2b", 2L);
			put("2k", 2 * SizeUnit.KB);
			put("2kb", 2 * SizeUnit.KB);
			put("2m", 2 * SizeUnit.MB);
			put("2mb", 2 * SizeUnit.MB);
			put("2g", 2 * SizeUnit.GB);
			put("2gb", 2 * SizeUnit.GB);
			put("2t", 2 * SizeUnit.TB);
			put("2tb", 2 * SizeUnit.TB);
		}};

		for (Map.Entry<String, Long> configEntry : configValues.entrySet()) {
			configuration.set(NiagaraConfiguration.MEMTABLE_SIZE.key(), configEntry.getKey());
			assertEquals(configuration.getMemtableSize(), configEntry.getValue().longValue());

			configuration.set(NiagaraConfiguration.BLOCK_CACHE_SIZE.key(), configEntry.getKey());
			assertEquals(configuration.getBlockCacheSize(), configEntry.getValue().longValue());
		}
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testSizeConfigurationWithErrorFormat() {
		NiagaraConfiguration configuration = new NiagaraConfiguration();

		configuration.set(NiagaraConfiguration.MEMTABLE_SIZE.key(), "3213hd");
		configuration.getMemtableSize();
	}

	@Test
	public void testGetNiagaraConf() {
		NiagaraConfiguration configuration = new NiagaraConfiguration();
		assertTrue(configuration.getNiagaraDBConf().isEmpty());
		assertTrue(configuration.getNiagaraLogConf().isEmpty());

		String fileSystemType = "niagara.tron_options.FileSystemOptions.filesystem_type";

		configuration.set(fileSystemType, "posix_sync");
		assertEquals("niagara.tron_options.FileSystemOptions.filesystem_type=posix_sync", configuration.getNiagaraDBConf());
		assertTrue(configuration.getNiagaraLogConf().isEmpty());
	}

	@Test
	public void testNiagaraDBLogPattern() {
		// Test niagara DB related configurations.
		Configuration configuration = new Configuration();
		String dbTestKey1 = "niagara.tron_options.TabletOptions.write_buffer_size_min";
		String dbTestKey2 = "niagara.A.FileSystemOptions.B";
		String dbTestKey3 = "niagara.FileSystemOptions.C";
		String dbTestKey4 = "niagara.E.F.G";
		configuration.setString(dbTestKey1, "1");
		configuration.setString(dbTestKey2, "2");
		configuration.setString(dbTestKey3, "3");
		configuration.setString(dbTestKey4, "4");
		NiagaraConfiguration niagaraConfDB = new NiagaraConfiguration(configuration);
		assertTrue(niagaraConfDB.getNiagaraDBConf().contains(dbTestKey1));
		assertTrue(niagaraConfDB.getNiagaraDBConf().contains(dbTestKey2));
		assertFalse(niagaraConfDB.getNiagaraDBConf().contains(dbTestKey3));
		assertFalse(niagaraConfDB.getNiagaraDBConf().contains(dbTestKey4));

		// Test alog related configurations.
		configuration = new Configuration();
		String logTestkey1 = "niagara.alog.a.b";
		String logTestkey2 = "niagara.alog.c";
		String logTestkey3 = "niagara.alog2.a.b";
		configuration.setString(logTestkey1, "1");
		configuration.setString(logTestkey2, "2");
		configuration.setString(logTestkey3, "3");
		NiagaraConfiguration niagaraConfLog = new NiagaraConfiguration(configuration);

		assertTrue(niagaraConfLog.getNiagaraLogConf().contains(logTestkey1));
		assertTrue(niagaraConfLog.getNiagaraLogConf().contains(logTestkey2));
		assertFalse(niagaraConfLog.getNiagaraLogConf().contains(logTestkey3));
	}

	private static class SizeUnit{
		static final long KB = 1024L;
		static final long MB = 1024L * 1024L;
		static final long GB = 1024L * 1024L * 1024L;
		static final long TB = 1024L * 1024L * 1024L * 1024L;
	}
}
