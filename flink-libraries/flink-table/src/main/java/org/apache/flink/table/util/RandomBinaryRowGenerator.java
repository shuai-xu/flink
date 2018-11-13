/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.util;

import org.apache.flink.table.api.types.BaseRowType;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;

import java.util.Random;

/**
 * BinaryRow random generator.
 */
public class RandomBinaryRowGenerator {

	private final BaseRowType rowType;
	private final BinaryRow row;
	private final BinaryRowWriter writer;
	private final Random rnd;

	public RandomBinaryRowGenerator(BaseRowType rowType) {
		this.rowType = rowType;
		this.row = new BinaryRow(rowType.getArity());
		this.writer = new BinaryRowWriter(row);
		this.rnd = new Random();
	}

	public BinaryRow generate() {
		writer.reset();
		for (int i = 0; i < rowType.getArity(); i++) {
			InternalType t = rowType.getTypeAt(i);
			if (DataTypes.BOOLEAN.equals(t)) {
				writer.writeBoolean(i, rnd.nextBoolean());
			} else if (DataTypes.BYTE.equals(t)) {
				writer.writeByte(i, (byte) rnd.nextInt());
			} else if (DataTypes.SHORT.equals(t)) {
				writer.writeShort(i, (short) rnd.nextInt());
			} else if (DataTypes.INT.equals(t)) {
				writer.writeInt(i, rnd.nextInt());
			} else if (DataTypes.LONG.equals(t)) {
				writer.writeLong(i, rnd.nextLong());
			} else if (DataTypes.FLOAT.equals(t)) {
				writer.writeFloat(i, rnd.nextFloat() * rnd.nextLong());
			} else if (DataTypes.DOUBLE.equals(t)) {
				writer.writeDouble(i, rnd.nextDouble() * rnd.nextLong());
			} else {
				throw new RuntimeException("Not support type: " + t);
			}
		}
		writer.complete();
		return row;
	}

}
