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

import java.util.Properties;

/**
 * Util to parse numbers.
 */
public class NumUtil {

	/**
	 * Get from properties, retun null if not exist.
	 * @param userProperties properties
	 * @param key key
	 * @return the value in this property, return null if not exist.
	 */
	public static String getProperty(Properties userProperties, String key) {
		return userProperties == null ? null : userProperties.getProperty(key);
	}

	/**
	 * Get from properties, retun null if not exist.
	 * @param userProperties properties
	 * @param key key
	 * @param defaultValue default value
	 * @return the value in this property, return null if not exist.
	 */
	public static double getProperty(Properties userProperties, String key, double defaultValue) {
		if (userProperties == null) {
			return defaultValue;
		}

		String value = userProperties.getProperty(key);
		return value == null ? defaultValue : parseDouble(value, defaultValue);
	}

	/**
	 * Parse string to long.
	 *
	 * @param str Input String
	 * @return Return parsed long value, When parse failed return -1
	 */
	public static long parseLong(String str) {
		long k;
		try {
			k = Long.parseLong(str);
		} catch (NumberFormatException e) {
			k = -1;
		}
		return k;
	}

	/**
	 * Parse string to int.
	 *
	 * @param str Input String
	 * @return Return parsed int value, When parse failed return -1
	 */
	public static int parseInt(String str) {
		int k;
		try {
			k = Integer.parseInt(str);
		} catch (NumberFormatException e) {
			k = -1;
		}
		return k;
	}

	/**
	 * Parse string to int with default value.
	 *
	 * @param str Input String
	 * @return Return parsed int value, When parse failed return defaultValue
	 */
	public static int parseInt(String str, int defaultValue) {
		int k;
		try {
			k = Integer.parseInt(str);
		} catch (NumberFormatException e) {
			k = defaultValue;
		}
		return k;
	}

	/**
	 * Parse string to double.
	 *
	 * @param str Input String
	 * @return Return parsed double value, When parse failed return -1
	 */
	public static double parseDouble(String str) {
		double k;
		try {
			k = Double.parseDouble(str);
		} catch (NumberFormatException e) {
			k = -1;
		}
		return k;
	}

	/**
	 * Parse string to double with default value.
	 *
	 * @param str Input String
	 * @return Return parsed double value, When parse failed return -1
	 */
	public static double parseDouble(String str, double defaultValue) {
		double k;
		try {
			k = Double.parseDouble(str);
		} catch (NumberFormatException e) {
			k = defaultValue;
		}
		return k;
	}

	/**
	 * Check if a string is an integer.
	 */
	public static boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
}
