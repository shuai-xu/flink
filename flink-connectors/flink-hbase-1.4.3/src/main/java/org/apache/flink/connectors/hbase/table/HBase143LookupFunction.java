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

import org.apache.flink.connectors.hbase.HTableSchema;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * This is an simple example adding some debug log.
 */
public class HBase143LookupFunction extends HBaseLookupFunction {

	private static final long serialVersionUID = -7860526753742670449L;
	private static final Logger LOG = LoggerFactory.getLogger(HBase143LookupFunction.class);

	public HBase143LookupFunction(HTableSchema schema, int rowKeyIndex, List<Integer> qualifierIndexes) throws IOException {
		// serialize default HBaseConfiguration from client's env
		this(schema, rowKeyIndex, qualifierIndexes, HBaseConfiguration.create());
	}

	public HBase143LookupFunction(
			HTableSchema schema,
			int rowKeyIndex,
			List<Integer> qualifierIndexes,
			org.apache.hadoop.conf.Configuration conf) throws IOException {
		super(schema, rowKeyIndex, qualifierIndexes, conf);
	}

	/**
	 * Override parent's eval method to add some debug log.
	 */
	@Override
	public void eval(Object rowKey) throws IOException {
		// fetch result
		Result result = table.get(createGet(rowKey));
		if (!result.isEmpty()) {
			// parse and collect
			collect(parseResult(result));
		} else {
			LOG.debug("could not retrieve any data for input key:{}", rowKey);
		}
	}
}
