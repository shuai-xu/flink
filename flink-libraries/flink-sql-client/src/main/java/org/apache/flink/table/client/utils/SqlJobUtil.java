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

package org.apache.flink.table.client.utils;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.runtime.functions.python.PythonUDFUtil;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.types.DecimalType;
import org.apache.flink.table.types.GenericType;
import org.apache.flink.table.types.InternalType;

import com.alibaba.blink.sql.parser.ddl.SqlColumnType;
import com.alibaba.blink.sql.parser.ddl.SqlCreateFunction;
import com.alibaba.blink.sql.parser.ddl.SqlCreateTable;
import com.alibaba.blink.sql.parser.ddl.SqlCreateTable.IndexWrapper;
import com.alibaba.blink.sql.parser.ddl.SqlNodeInfo;
import com.alibaba.blink.sql.parser.ddl.SqlTableColumn;
import com.alibaba.blink.sql.parser.impl.BlinkSqlParserImpl;
import com.alibaba.blink.sql.parser.plan.BlinkPlannerImpl;
import com.alibaba.blink.sql.parser.plan.SqlParseException;
import com.alibaba.blink.table.api.RichTableSchema;
import com.alibaba.blink.table.api.RichTableSchema.Index;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util to transform a sql context (a sequence of sql statements) into a flink job.
 */
public class SqlJobUtil {
	private static final Logger LOG = LoggerFactory.getLogger(SqlJobUtil.class);

	/**
	 * Configuration for sql parser.
	 */
	private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
		.setParserFactory(BlinkSqlParserImpl.FACTORY)
		.setQuoting(Quoting.BACK_TICK)
		.setQuotedCasing(Casing.UNCHANGED)
		.setUnquotedCasing(Casing.UNCHANGED)
		.setConformance(SqlConformanceEnum.DEFAULT)
		.setIdentifierMaxLength(128)
		.setLex(Lex.JAVA)
		.build();

	private static final SchemaPlus ROOT_SCHEMA = Frameworks.createRootSchema(true);

	private static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks
		.newConfigBuilder()
		.defaultSchema(ROOT_SCHEMA)
		.parserConfig(PARSER_CONFIG)
		.typeSystem(RelDataTypeSystem.DEFAULT)
		.build();

	/**
	 * Parses a sql context as a list of {@link SqlNodeInfo}.
	 *
	 * @throws SqlParseException if there is any syntactic error
	 */
	public static List<SqlNodeInfo> parseSqlContext(String sqlContext) throws SqlParseException {
		BlinkPlannerImpl planner = new BlinkPlannerImpl(FRAMEWORK_CONFIG);
		List<SqlNodeInfo> sqlNodeList = planner.parseContext(sqlContext);
		planner.validate(sqlNodeList);
		return sqlNodeList;
	}

	/**
	 * Registers functions to the tableEnvironment.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 * @param classLoader     the class loader to load user classes
	 * @return true or false
	 */
	public static boolean registerFunctions(
		TableEnvironment tableEnv,
		List<SqlNodeInfo> sqlNodeInfoList,
		ClassLoader classLoader,
		String userPyLibs) {

		Map<String, String> pyUdfNameClass = new HashMap<>();
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateFunction) {
				SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNodeInfo.getSqlNode();
				String functionName = sqlCreateFunction.getFunctionName().toString();

				boolean isPyUdf = sqlCreateFunction.getClassName().startsWith(PythonUDFUtil.PYUDF_PREFIX);
				if (isPyUdf) {
					String className = sqlCreateFunction.getClassName()
											.substring(PythonUDFUtil.PYUDF_PREFIX.length());
					pyUdfNameClass.put(functionName, className);
					continue;
				}

				// Register in catalog
				tableEnv.registerExternalFunction(
					null, functionName, sqlCreateFunction.getClassName());

				Object instance = UserDefinedFunctionUtils.createUserDefinedFunction(
					classLoader, functionName, sqlCreateFunction.getClassName());
				// Register in memory
				if (instance instanceof TableFunction) {
					TableFunction<?> tableFunction = (TableFunction) instance;
					tableEnv.registerFunction(functionName, tableFunction);
				} else if (instance instanceof AggregateFunction) {
					AggregateFunction<?, ?> aggregateFunction = (AggregateFunction) instance;
					tableEnv.registerFunction(functionName, aggregateFunction);
				} else if (instance instanceof ScalarFunction) {
					ScalarFunction scalarFunction = (ScalarFunction) instance;
					tableEnv.registerFunction(functionName, scalarFunction);
				} else {
					LOG.warn("Could not match the type of UDF class: {}", sqlCreateFunction.getClassName());
					throw new RuntimeException(
						TableErrors.INST.sqlRegisterUserDefinedFuncError(
							functionName,
							sqlCreateFunction.getClassName(),
							"Could not match the type of UDF class: " + sqlCreateFunction.getClassName()));
				}
			}
		}

		if (pyUdfNameClass.size() > 0) {
			ArrayList<String> pyFiles = new ArrayList<>(Arrays.asList(userPyLibs.split(",")));
			Map<String, UserDefinedFunction> pyUDFs =
				PythonUDFUtil.registerPyUdfsToTableEnvironment(tableEnv, pyFiles, pyUdfNameClass);
			// TODO How to handle the python function?
		}
		return true;
	}

	public static RichTableSchema createBlinkTableSchema(SqlCreateTable sqlCreateTable) {
		if (!"SOURCE".equals(sqlCreateTable.getTableType())) {
			if (sqlCreateTable.getWatermark() != null) {
				throw new IllegalArgumentException(
					TableErrors.INST.sqlTableTypeNotSupportWaterMark(
						sqlCreateTable.getTableName().toString(),
						sqlCreateTable.getTableType()));
			}
			if (sqlCreateTable.containsComputedColumn()) {
				throw new IllegalArgumentException(
					TableErrors.INST.sqlTableTypeNotSupportComputedCol(
						sqlCreateTable.getTableName().toString(),
						sqlCreateTable.getTableType()));
			}
		}

		//set columnList
		SqlNodeList columnList = sqlCreateTable.getColumnList();
		int columnCount = columnList.size();
		String[] columnNames = new String[columnCount];
		boolean[] nullables = new boolean[columnCount];
		InternalType[] columnTypes = new InternalType[columnCount];
		List<String> headerFields = new ArrayList<>();
		RichTableSchema schema;

		if (!sqlCreateTable.containsComputedColumn()) {
			// all column is SqlTableColumn
			for (int i = 0; i < columnCount; i++) {
				SqlTableColumn columnNode = (SqlTableColumn) columnList.get(i);
				String columnName = columnNode.getName().getSimple();
				columnNames[i] = columnName;
				try {
					columnTypes[i] = getInternalType(columnNode.getType());
				} catch (IllegalArgumentException e) {
					throw new IllegalArgumentException(
						TableErrors.INST.sqlUnSupportedColumnType(
							sqlCreateTable.getTableName().toString(),
							columnName,
							columnNode.getType().getTypeName().getSimple()));
				}
				nullables[i] = columnNode.getType().getNullable() == null ? true : columnNode.getType().getNullable();
				if (columnNode.isHeader()) {
					headerFields.add(columnName);
				}
			}
			schema = new RichTableSchema(columnNames, columnTypes, nullables);
		} else {
			// some columns are computed column
			List<String> originNameList = new ArrayList<>();
			List<InternalType> originTypeList = new ArrayList<>();
			List<Boolean> originNullableList = new ArrayList<>();
			for (int i = 0; i < columnCount; i++) {
				SqlNode node = columnList.get(i);
				if (node instanceof SqlTableColumn) {
					SqlTableColumn columnNode = (SqlTableColumn) columnList.get(i);
					String columnName = columnNode.getName().getSimple();
					try {
						InternalType columnType = getInternalType(columnNode.getType());
						originTypeList.add(columnType);
					} catch (IllegalArgumentException e) {
						throw new IllegalArgumentException(
							TableErrors.INST.sqlUnSupportedColumnType(
								sqlCreateTable.getTableName().toString(),
								columnName,
								columnNode.getType().getTypeName().getSimple()));
					}
					originNameList.add(columnName);
					originNullableList.add(columnNode.getType().getNullable() == null ? true : columnNode.getType().getNullable());
					if (columnNode.isHeader()) {
						headerFields.add(columnName);
					}
				}
			}
			String[] originColumnNames = originNameList.toArray(new String[originNameList.size()]);
			InternalType[] originColumnTypes = originTypeList.toArray(new InternalType[originTypeList.size()]);
			boolean[] originNullables = new boolean[originNullableList.size()];
			for (int i = 0; i < originNullables.length; i++) {
				originNullables[i] = originNullableList.get(i);
			}
			schema = new RichTableSchema(originColumnNames, originColumnTypes, originNullables);
		}

		schema.setHeaderFields(headerFields);

		//set primary key
		if (sqlCreateTable.getPrimaryKeyList() != null) {
			String[] primaryKeys = new String[sqlCreateTable.getPrimaryKeyList().size()];
			for (int i = 0; i < primaryKeys.length; i++) {
				primaryKeys[i] = sqlCreateTable.getPrimaryKeyList().get(i).toString();
			}
			schema.setPrimaryKey(primaryKeys);
		}

		//set unique key
		List<SqlNodeList> uniqueKeyList = sqlCreateTable.getUniqueKeysList();
		if (uniqueKeyList != null) {
			List<List<String>> ukList = new ArrayList<>();
			for (SqlNodeList uniqueKeys: uniqueKeyList) {
				List<String> uk = new ArrayList<>();
				for (int i = 0; i < uniqueKeys.size(); i++) {
					uk.add(uniqueKeyList.get(i).toString());
				}
				ukList.add(uk);
			}
			schema.setUniqueKeys(ukList);
		}

		//set index
		List<IndexWrapper> indexKeyList = sqlCreateTable.getIndexKeysList();
		if (indexKeyList != null) {
			List<Index> indexes = new ArrayList<>();
			for (IndexWrapper idx : indexKeyList) {
				List<String> keyList = new ArrayList<>();
				for (int i = 0; i < idx.indexKeys.size(); i++) {
					keyList.add(idx.indexKeys.get(i).toString());
				}
				indexes.add(new Index(idx.unique, keyList));
			}
			schema.setIndexes(indexes);
		}
		return schema;
	}

	/**
	 * Maps a sql column type to a flink {@link InternalType}.
	 *
	 * @param type the sql column type
	 * @return the corresponding flink type
	 */
	public static InternalType getInternalType(SqlDataTypeSpec type) {
		switch (SqlColumnType.getType(type.getTypeName().getSimple())) {
			case BOOLEAN:
				return DataTypes.BOOLEAN;
			case TINYINT:
				return DataTypes.BYTE;
			case SMALLINT:
				return DataTypes.SHORT;
			case INT:
				return DataTypes.INT;
			case BIGINT:
				return DataTypes.LONG;
			case FLOAT:
				return DataTypes.FLOAT;
			case DECIMAL:
				if (type.getPrecision() >= 0) {
					return DecimalType.of(type.getPrecision(), type.getScale());
				}
				return DecimalType.DEFAULT;
			case DOUBLE:
				return DataTypes.DOUBLE;
			case DATE:
				return DataTypes.DATE;
			case TIME:
				return DataTypes.TIME;
			case TIMESTAMP:
				return DataTypes.TIMESTAMP;
			case VARCHAR:
				return DataTypes.STRING;
			case VARBINARY:
				return DataTypes.BYTE_ARRAY;
			case ANY:
				return new GenericType<>(Object.class);
			default:
				LOG.warn("Unsupported sql column type: {}", type);
				throw new IllegalArgumentException("Unsupported sql column type " + type + " !");
		}
	}
}
