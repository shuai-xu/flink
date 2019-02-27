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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * DROP FUNCTION DDL sql call.
 */
public class SqlDropFunction extends SqlCall {

	private SqlNode functionName;
	private boolean ifExists;

	public SqlDropFunction(SqlParserPos pos, SqlNode functionName, boolean ifExists) {
		super(pos);
		this.functionName = functionName;
		this.ifExists = ifExists;
	}

	@Override
	public SqlKind getKind() {
		return SqlKind.OTHER_DDL;
	}

	@Override
	public SqlOperator getOperator() {
		return null;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return null;
	}

	public SqlNode getFunctionName() {
		return functionName;
	}

	public void setFunctionName(SqlNode functionName) {
		this.functionName = functionName;
	}

	public boolean getIfExists() {
		return this.ifExists;
	}

	public void setIfExists(boolean ifExists) {
		this.ifExists = ifExists;
	}

	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("DROP");
		writer.keyword("FUNCTION");
		if (ifExists) {
			writer.keyword("IF EXISTS");
		}
		functionName.unparse(writer, leftPrec, rightPrec);
	}

	public void validate() {
		//todo:
	}
}
