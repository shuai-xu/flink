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

package com.alibaba.blink.connectors.csv;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Csv options.
 */
public class CsvOptions {
	public static final ConfigOption<String> PATH = key("path".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<Boolean> OPTIONAL_ENUMERATE_NESTED_FILES =
			key("enumerateNestedFiles".toLowerCase()).defaultValue(true);

	public static final ConfigOption<String> OPTIONAL_FIELD_DELIM = key("fieldDelim".toLowerCase())
			.defaultValue(",");

	public static final ConfigOption<String> OPTIONAL_LINE_DELIM = key("lineDelim".toLowerCase())
			.defaultValue("\n");

	public static final ConfigOption<String> OPTIONAL_CHARSET = key("charset".toLowerCase())
			.defaultValue("UTF-8");

	public static final ConfigOption<Boolean> OPTIONAL_OVER_RIDE_MODE = key("override".toLowerCase())
			.defaultValue(true);

	public static final String PARAMS_HELP_MSG = String.format("required params:%s", PATH);

	public static final ConfigOption<Boolean> EMPTY_COLUMN_AS_NULL = key("emptyColumnAsNull".toLowerCase())
			.defaultValue(false);

	public static final ConfigOption<String> OPTIONAL_QUOTE_CHARACTER = key("quoteCharacter".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<Boolean> OPTIONAL_FIRST_LINE_AS_HEADER = key("firstLineAsHeader".toLowerCase())
			.defaultValue(false);

	public static final ConfigOption<Integer> PARALLELISM = key("parallelism".toLowerCase())
			.defaultValue(-1);

	public static final ConfigOption<String> TIME_ZONE_KEY = key("timezone".toLowerCase()).noDefaultValue();

}
