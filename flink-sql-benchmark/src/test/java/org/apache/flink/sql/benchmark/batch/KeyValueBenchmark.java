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

package org.apache.flink.sql.benchmark.batch;

import org.apache.flink.sql.benchmark.Benchmark;

import org.junit.Test;

/**
 * Benchmark KeyValue.
 */
public class KeyValueBenchmark extends BatchBenchmarkBase {

	@Test
	public void testKeyValue() {
		long n = 500L << 17;
		registerRange("T", n);
		Benchmark benchmark = new Benchmark("KeyValueBenchmark", n);
		String sql = "select KEYVALUE(str, '|', ':', 'k3') from " +
					"( select 'k1:v1|k2:v2|k3:v3' as str from T) x";
		benchmark.addCase("KEYVALUE", 3, () -> execute(sql));
		/*
		Java HotSpot(TM) 64-Bit Server VM 1.8.0_91-b14 on Mac OS X 10.11.6
		Intel(R) Core(TM) i5-5250U CPU @ 1.60GHz
		KeyValueBenchmark:                                           Best/Avg Time(ms)    Row Rate(M/s)      Per Row(ns)   Relative
		---------------------------------------------------------------------------------------------------------------------------
		OPERATORTEST_KeyValueBenchmark_keyvalue                           6786 / 9285              9.7            103.5       1.0X
		 */
		benchmark.run();
	}

}
