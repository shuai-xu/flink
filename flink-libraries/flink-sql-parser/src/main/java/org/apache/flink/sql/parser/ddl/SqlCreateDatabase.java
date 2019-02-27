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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * CREATE DATABASE DDL sql call.
 */
public class SqlCreateDatabase extends SqlCall {

	private final SqlIdentifier databaseName;

	private final SqlNodeList propertyList;

	private final SqlCharStringLiteral comment;

	public SqlCreateDatabase(
		SqlParserPos position,
		SqlIdentifier databaseName,
		SqlNodeList propertyList,
		SqlCharStringLiteral comment) {
		super(position);
		this.databaseName = databaseName;
		this.propertyList = propertyList;
		this.comment = comment;
	}

	public SqlIdentifier getDatabaseName() {
		return databaseName;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	public SqlCharStringLiteral getComment() {
		return comment;
	}

	@Override
	public SqlOperator getOperator() {
		return null;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return null;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		writer.keyword("DATABASE");
		databaseName.unparse(writer, leftPrec, rightPrec);

		// database comment
		if (comment != null) {
			writer.keyword("COMMENT");
			comment.unparse(writer, leftPrec, rightPrec);
			writer.newlineAndIndent();
		}

		// database properties
		if (propertyList != null) {
			writer.keyword("WITH");
			SqlWriter.Frame withFrame = writer.startList("(", ")");
			for (SqlNode property : propertyList) {
				writer.sep(",", false);
				writer.newlineAndIndent();
				writer.print("  ");
				property.unparse(writer, leftPrec, rightPrec);
			}
			writer.newlineAndIndent();
			writer.endList(withFrame);
		}
	}

	@Override
	public void validate(SqlValidator validator, SqlValidatorScope scope) {
		// TODO: validate create dababase
	}
}
