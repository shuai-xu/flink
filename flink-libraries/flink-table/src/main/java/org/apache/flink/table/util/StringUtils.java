/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.table.dataformat.BinaryString;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * String utils.
 */
public class StringUtils {
	private static final List<BinaryString> TRUE_STRINGS =
		Stream
			.of("t", "true", "y", "yes", "1")
			.map(BinaryString::fromString)
			.collect(Collectors.toList());
	private static final List<BinaryString> FALSE_STRINGS =
		Stream
			.of("f", "false", "n", "no", "0")
			.map(BinaryString::fromString)
			.collect(Collectors.toList());

	/**
	 * Decide boolean representation of a string.
	 * @param s1 string to decide from.
	 * @return boolean value if matches as desired.
	 */
	public static Boolean stringToBoolean(BinaryString s1) {
		if (null == s1) {
			return null;
		}
		if (isTrueString(s1)) {
			return true;
		} else if (isFalseString(s1)) {
			return false;
		} else {
			// we should return null here.
			return null;
		}
	}

	/**
	 * Decide if a string represents boolean true in semanteme, mainly used in type coercion.
	 * @param s1 string to decide from.
	 * @return true if the string value matches as desired.
	 */
	private static boolean isTrueString(BinaryString s1) {
		return TRUE_STRINGS.contains(s1.toLowerCase());
	}

	/**
	 * Decide if a string represents boolean false in semanteme, mainly used in type coercion.
	 * @param s1 string to decide from.
	 * @return true if the string value matches as desired.
	 */
	private static boolean isFalseString(BinaryString s1) {
		return FALSE_STRINGS.contains(s1.toLowerCase());
	}
}
