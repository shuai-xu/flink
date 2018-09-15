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

package com.alibaba.blink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Watermark DDL sql call.
 */
public class SqlWatermark extends SqlCall {

	private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("WATERMARK", SqlKind.OTHER);

	private SqlIdentifier watermarkName;
	private SqlIdentifier columnName;
	private SqlCall functionCall;

	public SqlWatermark(SqlIdentifier watermarkName, SqlIdentifier columnName, SqlNode functionCall, SqlParserPos pos) {
		super(pos);
		this.watermarkName = watermarkName;
		this.columnName = requireNonNull(columnName, "Column name is missing in watermark clause");
		this.functionCall = requireNonNull((SqlCall) functionCall, "Function call is missing in watermark, clause");
	}

	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(watermarkName, columnName, functionCall);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("WATERMARK");
		if (watermarkName != null) {
			watermarkName.unparse(writer, leftPrec, rightPrec);
		}
		writer.keyword("FOR");
		columnName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		functionCall.unparse(writer, leftPrec, rightPrec);
	}

	@Override
	public void validate(SqlValidator validator, SqlValidatorScope scope) {
	}

	public SqlIdentifier getWatermarkName() {
		return watermarkName;
	}

	public SqlIdentifier getColumnName() {
		return columnName;
	}

	public SqlCall getFunctionCall() {
		return functionCall;
	}
}
