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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_NOREUSE;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_NORMAL;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_WILLNEED;

/**
 * The policy used to decide how to manage OS cache relate to external shuffle data.
 */
public enum OsCachePolicy {
	/**
	 * No special treatment.
	 */
	NO_TREATMENT,

	/**
	 * Expect pages of a limited size of data after current position to be referenced.
	 * Expect no more page reference after the subpartition has been fully consumed.
	 */
	READ_AHEAD,

	/**
	 * Expect page references in sequential order while reading the subpartition.
	 * Expect no more page reference after the subpartition has been fully consumed.
	 */
	SEQUENTIAL,

	/**
	 * Expect data to be accessed only once.
	 * Expect no more page reference after the subpartition has been fully consumed.
	 */
	NO_REUSE;

	@Override
	public String toString() {
		switch (this) {
			case READ_AHEAD:
				return "read-ahead";
			case SEQUENTIAL:
				return "sequential";
			case NO_REUSE:
				return "no-reuse";
			default:
				return "no-treatment";
		}
	}

	int getFadviceFlag() {
		switch(this) {
			case READ_AHEAD:
				return POSIX_FADV_WILLNEED;
			case SEQUENTIAL:
				return POSIX_FADV_SEQUENTIAL;
			case NO_REUSE:
				return POSIX_FADV_NOREUSE;
			default: // case NO_TREATMENT
				return POSIX_FADV_NORMAL;
		}
	}

	static String getSupportedOsCachePolicies() {
		StringBuilder stringBuilder = new StringBuilder(45);
		for (OsCachePolicy policy : OsCachePolicy.values()) {
			stringBuilder.append(policy.toString()).append(",");
		}
		return stringBuilder.substring(0, stringBuilder.length() - 1);
	}

	static OsCachePolicy getOsCachePolicyFromConfiguration(final Configuration configuration, Logger logger) {
		OsCachePolicy osCachePolicy = OsCachePolicy.NO_TREATMENT;
		String strOsCachePolicy = configuration.getString(ExternalBlockShuffleServiceOptions.OS_CACHE_POLICY);
		for (OsCachePolicy policy : OsCachePolicy.values()) {
			if (policy.toString().equals(strOsCachePolicy)) {
				osCachePolicy = policy;
			}
		}
		if (!osCachePolicy.equals(OsCachePolicy.NO_TREATMENT) && !NativeIO.isAvailable()) {
			logger.warn("Current OS cache policy [" + osCachePolicy + "] cannot take effect. " +
				"Make sure the native-hadoop library is in the library path. Use no-treatment policy instead.");
			osCachePolicy = OsCachePolicy.NO_TREATMENT;
		}
		return osCachePolicy;
	}
}
