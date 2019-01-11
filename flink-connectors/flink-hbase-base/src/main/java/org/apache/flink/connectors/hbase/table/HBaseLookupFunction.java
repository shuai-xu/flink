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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.HTableSchema;
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer;
import org.apache.flink.connectors.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connectors.hbase.util.HBaseTypeUtils;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The HBaseLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL.
 */
public class HBaseLookupFunction extends TableFunction<Row> {

	private static final long serialVersionUID = 6552257501168286801L;
	private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupFunction.class);

	protected final int totalQualifiers;
	protected final int rowKeyIndex;
	protected final int rowKeyInternalTypeIndex;
	protected final TypeInformation rowKeyType;
	protected final List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList;
	protected final List<Integer> qualifierIndexes;

	private HTableSchema schema;
	private Map<String, String> params;
	private byte[] serializedConfig;
	protected List<HBaseBytesSerializer> serializers;

	private transient Connection hConnection;
	protected transient HTable table;

	public HBaseLookupFunction(HTableSchema schema, int rowKeyIndex, List<Integer> qualifierIndexes) throws IOException {
		// serialize default HBaseConfiguration from client's env
		this(schema, rowKeyIndex, qualifierIndexes, HBaseConfiguration.create());
	}

	public HBaseLookupFunction(
			HTableSchema schema,
			int rowKeyIndex,
			List<Integer> qualifierIndexes,
			org.apache.hadoop.conf.Configuration conf) throws IOException {
		this.schema = schema;
		this.totalQualifiers = qualifierIndexes.size();
		Preconditions.checkArgument(
				rowKeyIndex > -1 && totalQualifiers > rowKeyIndex,
				"rowKeyIndex must > -1 and totalQualifiers number must > rowKeyIndex");
		qualifierList = schema.getFlatQualifiers();
		Preconditions.checkArgument(
				totalQualifiers == qualifierList.size(),
				"totalQualifiers number must equal to qualifier numbers defined in HBaseSchema");

		// currently rowKey always be a binary type (from sql-level type system)
		this.rowKeyIndex = rowKeyIndex;
		this.rowKeyType = qualifierList.get(rowKeyIndex).f2;
		this.rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(rowKeyType);

		this.qualifierIndexes = qualifierIndexes;
		// Configuration is not serializable
		this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
		this.serializers = new ArrayList<>();

		for (int index = 0; index < totalQualifiers; index++) {
			Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(index);
			serializers.add(new HBaseBytesSerializer(typeInfo.f2));
		}
	}

	public HBaseLookupFunction withParam(String key, String value) {
		if (params == null) {
			params = new HashMap<>();
		}
		params.put(key, value);
		return this;
	}

	protected Get createGet(Object rowKey) throws IOException {
		// currently this rowKey always be a binary type (adapt to sql-level type system)
		byte[] rowkey = HBaseTypeUtils.serializeFromInternalObject(rowKey, rowKeyInternalTypeIndex, HBaseTypeUtils.DEFAULT_CHARSET);

		Get get = new Get(rowkey);
		get.setMaxVersions(1);
		for (int index = 0; index < totalQualifiers; index++) {
			int qualifierIndex = qualifierIndexes.get(index);
			Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(qualifierIndex);
			get.addColumn(typeInfo.f0, typeInfo.f1);
		}
		return get;
	}

	public void eval(Object rowKey) throws IOException {
		// fetch result
		Result result = table.get(createGet(rowKey));
		if (!result.isEmpty()) {
			// parse and collect
			collect(parseResult(result));
		}
	}

	protected Row parseResult(Result result) {
		// output qualifiers + rowKey
		Row row = new Row(totalQualifiers + 1);
		for (int index = 0; index <= totalQualifiers; index++) {

			if (index != rowKeyIndex) {
				Tuple3<byte[], byte[], TypeInformation<?>> qInfo = qualifierList.get(index);
				byte[] value = result.getValue(qInfo.f0, qInfo.f1);
				row.setField(index, serializers.get(index).fromHBaseBytes(value));
			} else {
				row.setField(rowKeyIndex, serializers.get(rowKeyIndex).fromHBaseBytes(result.getRow()));
			}
		}
		return row;
	}

	org.apache.hadoop.conf.Configuration prepareRuntimeConfiguration() throws IOException {
		// create default configuration from current runtime env (`hbase-site.xml` in classpath) first,
		// and overwrite configuration using serialized configuration from client-side env (`hbase-site.xml` in classpath).
		org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create());

		// user params from client-side have the highest priority
		if (null != params) {
			for (Map.Entry<String, String> entry : params.entrySet()) {
				runtimeConfig.set(entry.getKey(), entry.getValue());
				LOG.info(String.format("Add user-defined param [%s]:[%s]", entry.getKey(), entry.getValue()));
			}
		}
		// do validation: check key option(s) in final runtime configuration
		if (StringUtils.isNullOrWhitespaceOnly(runtimeConfig.get(HConstants.ZOOKEEPER_QUORUM))) {
			LOG.error(String.format("can not connect to hbase without {%s} configuration"), HConstants.ZOOKEEPER_QUORUM);
			throw new IOException("check hbase configuration failed, lost: '" + HConstants.ZOOKEEPER_QUORUM + "'!");
		}

		return runtimeConfig;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		LOG.info("start open ...");
		String tableName = schema.getTableName();
		org.apache.hadoop.conf.Configuration config = prepareRuntimeConfiguration();
		try {
			hConnection = ConnectionFactory.createConnection(config);
			table = (HTable) hConnection.getTable(TableName.valueOf(tableName));
		} catch (TableNotFoundException tnfe) {
			LOG.error("Table '{}' not found ", tableName, tnfe);
			throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to HBase.", ioe);
			throw new RuntimeException("Cannot create connection to HBase.", ioe);
		}
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		if (null != table) {
			try {
				table.close();
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close table", e);
			}
		}
		if (null != hConnection) {
			try {
				hConnection.close();
			} catch (IOException e) {
				// ignore exception when close.
				LOG.warn("exception when close connection", e);
			}
		}
		LOG.info("end close.");
	}
}
