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

package org.apache.flink.connectors.hbase.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hbase.HTableSchema;
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The HBase143Writer implements the sink function that can be used both in bounded and unbounded stream.
 */
public class HBase143Writer extends HBaseWriterBase<Tuple2<Boolean, Row>> {
	private static final Logger LOG = LoggerFactory.getLogger(HBase143Writer.class);

	final int totalQualifiers;
	final int rowKeyIndex;
	final List<Tuple3<byte[], byte[], TypeInformation<?>>> qualifierList;
	final List<Integer> qualifierIndexes;
	List<HBaseBytesSerializer> serializers;

	public HBase143Writer(HTableSchema schema, int rowKeyIndex, List<Integer> qualifierIndexes) throws IOException {
		super(schema);
		this.totalQualifiers = qualifierIndexes.size();
		Preconditions.checkArgument(rowKeyIndex > -1 && totalQualifiers > rowKeyIndex,
			"rowKeyIndex must > -1 and totalQualifiers number must > rowKeyIndex");
		qualifierList = schema.getFlatQualifiers();
		Preconditions.checkArgument(totalQualifiers == qualifierList.size(),
			"totalQualifiers number must equal to qualifier numbers defined in HBaseSchema");
		this.rowKeyIndex = rowKeyIndex;
		this.qualifierIndexes = qualifierIndexes;
		this.serializers = new ArrayList<>();

		for (int index = 0; index <= totalQualifiers; index++) {
			if (index != rowKeyIndex) {
				Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(index);
				serializers.add(new HBaseBytesSerializer(typeInfo.f2));
			} else {
				serializers.add(new HBaseBytesSerializer(qualifierList.get(rowKeyIndex).f2));
			}
		}
	}

	@Override
	public void invoke(Tuple2<Boolean, Row> row, Context context) throws Exception {
		if (null == row) {
			return;
		}
		if (row.getArity() != totalQualifiers + 1) {
			LOG.warn("discard invalid row:{}", row);
		} else {
			byte[] rowkey = serializers.get(rowKeyIndex).toHBaseBytes(row.getField(rowKeyIndex));
			if (row.f0) {
				// upsert
				Put put = new Put(rowkey);
				for (int index = 0; index <= totalQualifiers; index++) {
					if (index != rowKeyIndex) {
						int qualifierIndex = qualifierIndexes.get(index);
						Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(qualifierIndex);
						byte[] value = serializers.get(index).toHBaseBytes(row.getField(index));
						put.addColumn(typeInfo.f0, typeInfo.f1, value);
					}
				}
				table.put(put);
			} else {
				// delete
				Delete delete = new Delete(rowkey);
				for (int index = 0; index <= totalQualifiers; index++) {
					if (index != rowKeyIndex) {
						Tuple3<byte[], byte[], TypeInformation<?>> typeInfo = qualifierList.get(index);
						delete.addColumn(typeInfo.f0, typeInfo.f1);
					}
				}
				table.delete(delete);
			}
		}
	}

	@Override
	public String toString() {
		return HBase143Writer.class.getSimpleName() + ":{" + schema.getTableName() + "}";
	}
}
