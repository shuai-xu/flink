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

package com.alibaba.blink.launcher;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * JsonSimplify is used to simplify operator names.
 */
public class JsonSimplify {

	public static Set<Character> beginEatChar = new HashSet<Character>();
	public static Set<Character> stopEatChar = new HashSet<Character>();

	static {
		beginEatChar.add(')');
		beginEatChar.add(',');
		beginEatChar.add(' ');

		stopEatChar.add('(');
		stopEatChar.add(':');
	}

	/**
	 * Selected fields will not be displayed, so remove them.
	 */
	public static String removeField(String name) {
		char[] input = name.toCharArray();
		int len = input.length;
		int deleteNum = 0;
		boolean eatFlag = true;
		int index = len;
		while (index > 0) {
			index--;
			if (beginEatChar.contains(input[index])) {
				eatFlag = true;
			} else if (stopEatChar.contains(input[index])) {
				eatFlag = false;
			} else if (eatFlag) {
				input[index] = 0;
				deleteNum++;
			}
		}

		char[] res = new char[len - deleteNum];
		index = 0;
		for (int i = 0; i < len; ++i) {
			if (input[i] != 0) {
				res[index++] = input[i];
			}
		}
		return String.valueOf(res);
	}

	private static final Pattern P1 = Pattern.compile("\\$\\$");

	/**
	 * Display Class Simply name.
	 */
	public static String simplifyUdxName(String name) {
		name = P1.matcher(name).replaceAll("\\$");
		char[] res = name.toCharArray();
		// simplify udx name
		boolean udxFlag = false;
		int blankIdx = -1, firstDollar = -1, secondeDollar = -1;
		for (int i = res.length - 1; i >= 0; --i) {
			if (res[i] == '(') {
				udxFlag = true;
				blankIdx = i;
			} else if (beginEatChar.contains(res[i]) || stopEatChar.contains(res[i])) {
				blankIdx = -1;
				firstDollar = -1;
				secondeDollar = -1;
				udxFlag = false;
			} else if (udxFlag && res[i] == '$' && firstDollar == -1) {
				firstDollar = i;
			} else if (udxFlag && res[i] == '$' && firstDollar != -1 && secondeDollar == -1) {
				secondeDollar = i;
				res[blankIdx] = ' ';
				res[firstDollar] = '(';
				res[secondeDollar] = ' ';
				blankIdx = -1;
				firstDollar = -1;
				secondeDollar = -1;
				udxFlag = false;
			}
		}
		return String.valueOf(res);
	}

	private static final Pattern P2 = Pattern.compile(",,+");
	private static final Pattern P3 = Pattern.compile("\\(,");
	private static final Pattern P4 = Pattern.compile(",\\)");

	public static String removeDuplicateComma(String name) {
		name = P2.matcher(name).replaceAll(",");
		name = P3.matcher(name).replaceAll("(");
		name = P4.matcher(name).replaceAll(")");
		return name;
	}

	private static final Pattern P5 = Pattern.compile("\\(");
	private static final Pattern P6 = Pattern.compile("\\)");
	private static final Pattern P7 = Pattern.compile(",,+");
	private static final Pattern P8 = Pattern.compile("\\(,\\)");

	/**
	 * Deduplicated continuous same udx.
	 */
	public static String deduplicatUDX(String name) {
		name = P5.matcher(name).replaceAll("(,");
		name = P6.matcher(name).replaceAll(",)");
		name = P7.matcher(name).replaceAll(",");
		name = P8.matcher(name).replaceAll("()");
		String[] array = name.split(",");
		for (int i = 1; i < array.length; ++i) {
			if (array[i].equals(array[i - 1])) {
				array[i - 1] = "";
			}
		}

		StringBuilder res = new StringBuilder();
		for (int i = 0; i < array.length; ++i) {
			if (array[i].equals("")) {
				continue;
			}
			if (res.length() != 0) {
				res.append(",");
			}
			res.append(array[i]);
		}
		return res.toString();
	}

	/**
	 * Process single operator name.
	 */
	public static String processName(String name) {
		try {
			if (name == null) {
				name = "";
			}
			name = simplifyUdxName(name);
			name = removeField(name);
			name = name.replace(" ", "");
			name = deduplicatUDX(name);
			name = removeDuplicateComma(name);
			return name;
		} catch (Exception e) {
			System.out.println(e);
			return name;
		}
	}

}
