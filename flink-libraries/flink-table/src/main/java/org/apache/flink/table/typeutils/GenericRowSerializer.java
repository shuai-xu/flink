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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.util.BaseRowUtil;

import java.io.IOException;

/**
 * Serializer for GenericRow.
 */
@Deprecated
public class GenericRowSerializer extends BaseRowSerializer<GenericRow> {
	private final TypeSerializer[] typeSerializers;

	public GenericRowSerializer(TypeInformation<?>... types) {
		super(GenericRow.class, types);

		this.typeSerializers = new TypeSerializer[types.length];
		for (int i = 0; i < types.length; i++) {
			this.typeSerializers[i] = TypeUtils.createSerializer(types[i]);
		}
	}

	@Override
	public void serialize(BaseRow row, DataOutputView target) throws IOException {
		if (row instanceof BinaryRow) {
			binarySerializer.serialize((BinaryRow) row, target);
		} else {
			super.serialize(row, target);
		}
	}

	@Override
	public GenericRow deserialize(DataInputView source) throws IOException {
		BaseRow binaryRow = super.deserialize(source);
		return BaseRowUtil.toGenericRow(binaryRow, types, typeSerializers);
	}

	@Override
	public TypeSerializer<GenericRow> duplicate() {
		return new GenericRowSerializer(types);
	}
}
