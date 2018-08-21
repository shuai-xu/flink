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

package org.apache.flink.table.runtime.operator;

import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.table.codegen.Projection;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.util.BinaryRowUtil;

import java.io.Serializable;

/**
 */
public class AggregateMapKeyComparator
		extends TypePairComparator<BinaryRow, BaseRow> implements Serializable {

	private static final long serialVersionUID = 1L;

	protected Projection<BaseRow, BinaryRow> build;
	protected BinaryRow probeKey;

	public AggregateMapKeyComparator(Projection build) {
		this.build = build;
	}

	@Override
	public void setReference(BinaryRow binaryRow) {
		probeKey = binaryRow;
	}

	@Override
	public boolean equalToReference(BaseRow candidate) {
		BinaryRow buildKey = build.apply(candidate);
		// They come from Projection, so we can make sure it is in byte[].
		return buildKey.getSizeInBytes() == probeKey.getSizeInBytes()
				&& BinaryRowUtil.byteArrayEquals(
				buildKey.getMemorySegment().getHeapMemory(),
				probeKey.getMemorySegment().getHeapMemory(),
				buildKey.getSizeInBytes());
	}

	@Override
	public int compareToReference(BaseRow candidate) {
		throw new UnsupportedOperationException("Group key comparision unsupported.");
	}
}
