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

package org.apache.flink.table.sinks.csv;

import org.apache.flink.api.java.io.AbstractCsvOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.conversion.DataStructureConverters;
import org.apache.flink.table.types.InternalType;

/**
 * BaseRow csv output format.
 */
public class BaseRowCsvOutputFormat extends AbstractCsvOutputFormat<BaseRow> {
	private static final long serialVersionUID = 1L;

	private final InternalType[] fieldTypes;
	private DataStructureConverters.DataStructureConverter[] converters;

	public BaseRowCsvOutputFormat(Path outputPath, InternalType[] fieldTypes) {
		super(outputPath);
		this.fieldTypes = fieldTypes;
		this.converters = new DataStructureConverters.DataStructureConverter[fieldTypes.length];
	}

	@Override
	protected Object getSpecificField(BaseRow record, int n) {
		if (converters[n] == null) {
			converters[n] = DataStructureConverters.getConverterForType(fieldTypes[n]);
		}
		return converters[n].toExternal(record, n);
	}

	@Override
	protected int getFieldsNum(BaseRow record) {
		return record.getArity();
	}
}
