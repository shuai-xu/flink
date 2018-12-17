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

package org.apache.flink.table.temptable;

import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Unit test for {@link TableStorage}.
 */
public class TableStorageTest {

	@Test
	public void testWriteAndRead() throws Exception {
		File dir = File.createTempFile("flink_table_storage", System.currentTimeMillis() + "");
		if (dir.exists()) {
			dir.delete();
		}
		dir.mkdirs();
		dir.deleteOnExit();
		TableStorage tableStorage = new TableStorage(dir.getAbsolutePath());

		BinaryRow binaryRow = new BinaryRow(5);
		BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
		writer.writeInt(0, 100);
		writer.writeLong(1, 12345L);
		writer.writeString(2, "Hello World");
		writer.writeBoolean(3, false);
		writer.writeDouble(4, 1.123);
		writer.complete();
		byte[] bytes = BinaryRowUtil.copy(binaryRow.getAllSegments(), binaryRow.getBaseOffset(), binaryRow.getSizeInBytes());
		tableStorage.write("table1", 0, bytes);
		byte[] readBytes = new byte[bytes.length];
		tableStorage.read("table1", 0, 0, bytes.length, readBytes);
		Assert.assertArrayEquals(bytes, readBytes);
	}

}
