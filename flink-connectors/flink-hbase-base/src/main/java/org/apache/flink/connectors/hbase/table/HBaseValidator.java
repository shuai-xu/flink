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

package org.apache.flink.connectors.hbase.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The validator for HBase.
 */
@Internal
public class HBaseValidator extends ConnectorDescriptorValidator {
	public static final String DEFAULT_COLUMN_FAMILY = "cf";
	public static final String DEFAULT_ROW_KEY = "rowkey";

	public static final String CONNECTOR_TYPE_VALUE_HBASE = "HBASE";
	public static final String CONNECTOR_VERSION_VALUE_143 = "1.4.3";
	public static final String CONNECTOR_VERSION_VALUE_211 = "2.1.1";
	public static final String CONNECTOR_ZK_QUORUM = "hbase.zookeeper.quorum";
	public static final String CONNECTOR_HBASE_TABLE_NAME = "tableName";

	public static final String CONNECTOR_HBASE_CLIENT_PARAM_PREFIX = "hbase.*";
	public static final String CONNECTOR_COLUMN_FAMILY = "columnFamily";
	public static final String CONNECTOR_COLUMN_MAPPING = "columnMapping";

	// async table api is supported from version 2.0.0
	public static final String CONNECTOR_ASYNC_MODE = "async";
	public static final String CONNECTOR_BATCH_WRITE_SIZE = "batchSize";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE, false);
		validateVersion(properties);
		validateConnectionProperties(properties);
	}

	private void validateVersion(DescriptorProperties properties) {
		final List<String> versions = Arrays.asList(
				CONNECTOR_VERSION_VALUE_143,
				CONNECTOR_VERSION_VALUE_211);
		properties.validateEnumValues(CONNECTOR_VERSION, false, versions);
	}

	private void validateConnectionProperties(DescriptorProperties properties) {
		final Map<String, Consumer<String>> propertyValidators = new HashMap<>();
		propertyValidators.put(
				CONNECTOR_ZK_QUORUM,
				key -> properties.validateString(key, false, 1));
		properties.validateFixedIndexedProperties(CONNECTOR_ZK_QUORUM, true, propertyValidators);
	}
}
