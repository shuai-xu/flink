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

import org.apache.commons.lang3.StringUtils;

import java.util.function.Supplier;

/**
 *
 */
public class StringUtil {

	/**
	 * Filter some special character such as ",\.
	 *
	 * @param s Input String
	 * @return Filtered String
	 */
	public static String filterSpecChars(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
				case '\"':
					sb.append("\\\"");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				case '/':
					sb.append("\\/");
					break;
				case '\b':
					sb.append("\\b");
					break;
				case '\f':
					sb.append("\\f");
					break;
				case '\n':
					sb.append("\\n");
					break;
				case '\r':
					sb.append("\\r");
					break;
				case '\t':
					sb.append("\\t");
					break;
				default:
					sb.append(c);
			}
		}
		return sb.toString();
	}

	public static boolean isEmpty(String... strs) {
		for (String str : strs) {
			if (StringUtils.isEmpty(str)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Delay computation of object and object.toString() until toString() is called.
	 */
	public static LazyToString lazyToString(Supplier<Object> objectSupplier) {
		return new LazyToString(objectSupplier);
	}

	private static class LazyToString {
		private Supplier<Object> supplier;

		public LazyToString(Supplier<Object> objectSupplier) {
			this.supplier = objectSupplier;
		}

		@Override
		public String toString() {
			return supplier.get().toString();
		}
	}

}
