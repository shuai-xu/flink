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

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * Math Util.
 */
public class MathUtil {

	public static double roundToSigDigits(double d, int significantDigits) {
		BigDecimal bd = new BigDecimal(d);
		bd = bd.round(new MathContext(significantDigits));
		return bd.doubleValue();
	}

	public static int distribute(int total, int n, boolean takeRemaining) {
		int x = total / n;
		return takeRemaining ? total - x * (n - 1) : x;
	}

	public static int ceilDiv(double x, double y) {
		return (int) Math.ceil(x / y);
	}

	public static int floorDiv(double x, double y) {
		return (int) Math.floor(x / y);
	}
}
