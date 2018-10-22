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

/**
 * utils for env variable handling.
 */
public class EnvUtil {

	/**
	 * enum to represent system property name for appender.
	 */
	public enum AttrForAppender {
		/**
		 * job name property.
		 */
		JOB_NAME_ATTR("_JOB_NAME"),

		/**
		 * cluster name property.
		 */
		CLUSTER_NAME_ATTR("__inner__clusterName__"),

		/**
		 * queue name property.
		 */
		QUEUE_NAME_ATTR("__inner__queueName__"),

		/**
		 * appender level property.
		 */
		APPENDER_LEVEL_ATTR("__inner__appendLv__");

		private String name;

		AttrForAppender(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	public static void addAttrForLogAppender(AttrForAppender attr, String value) {
		if (value != null && !value.isEmpty()) {
			if (System.getProperty(attr.getName()) != null) {
				if (System.getProperty(attr.getName()) != value) {
					throw new IllegalArgumentException("This seems a bug. Please file an issue.");
				}
			} else {
				System.setProperty(attr.getName(), value);
			}
		}
	}

}
