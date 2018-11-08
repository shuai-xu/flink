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

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * A simple utility class to support trim trailing white spaces when loading properties file.
 */
public class PropertiesUtil {

	public static Properties loadWithTrimmedValues(InputStream inputStream) throws IOException {
		Properties prop = new Properties();
		loadWithTrimmedValues(inputStream, prop);
		return prop;
	}

	public static void loadWithTrimmedValues(InputStream inputStream, Properties prop) throws IOException {
		if (null == inputStream || null == prop) {
			return;
		}
		Properties rawProp = new Properties();
		rawProp.load(inputStream);
		for (Enumeration propKeys = rawProp.propertyNames(); propKeys.hasMoreElements(); ) {
			String trimmedKey = ((String) propKeys.nextElement()).trim();
			String trimmedValue = rawProp.getProperty(trimmedKey).trim();
			prop.put(trimmedKey, trimmedValue);
		}
	}
}
