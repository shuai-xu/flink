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

package com.alibaba.blink.launcher.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.factories.FlinkTableFactory;
import org.apache.flink.sql.parser.ddl.SqlAnalyzeTable;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.ddl.SqlRichDescribeTable;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.plan.FlinkPlannerImpl;
import org.apache.flink.sql.parser.plan.SqlParseException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableProperties;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.client.utils.SqlJobUtil;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.hive.functions.HiveFunctionWrapper;
import org.apache.flink.table.hive.functions.HiveGenericUDF;
import org.apache.flink.table.hive.functions.HiveGenericUDTF;
import org.apache.flink.table.hive.functions.HiveSimpleUDF;
import org.apache.flink.table.hive.functions.HiveUDAFFunction;
import org.apache.flink.table.plan.stats.AnalyzeStatistic;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.runtime.functions.python.PythonUDFUtil;
import org.apache.flink.table.sinks.RetractCsvTableSink;
import org.apache.flink.table.sinks.RetractMergeCsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertCsvTableSink;
import org.apache.flink.table.sinks.csv.CsvTableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.util.DescribeTableColumn;
import org.apache.flink.table.util.SerializationUtil;
import org.apache.flink.table.util.WatermarkUtils;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import com.alibaba.blink.launcher.ConfConstants;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlProperty;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter to transform a sql context (a sequence of sql statement) into a flink job.
 */
public class SqlJobAdapter {
	private static final Logger LOG = LoggerFactory.getLogger(SqlJobAdapter.class);

	private static final String START_TIME_KEY = "starttime";
	private static final String TIME_ZONE_KEY = "timezone";
	private static final String START_TIME_MS_KEY = "starttimems";
	private static final String STS_ROLE_ARN = "rolearn";
	private static final String SOURCE_START_TIME = "blink.job.source.startTime";
	private static final String SOURCE_START_TIME_MS = "blink.job.source.startTime.mills";
	private static final String STS_UID = "stsUid";
	private static final String STS_REGIONID = "stsRegionId";
	private static final String STS_ACCESSID = "stsAccessId";
	private static final String STS_ACCESSKEY = "stsAccesskey";
	private static Pattern pattern = Pattern.compile("acs:ram::(.*?):role/aliyunstreamdefaultrole");
	private static final String BRS_END_POINT = "brsEndPoint";

	private static final String BLINK_ENVIRONMENT_TYPE_KEY = "blinkEnvironmentTypeKey";
	private static final String BLINK_ENVIRONMENT_STREAM_VALUE = "stream";
	private static final String BLINK_ENVIRONMENT_BATCHEXEC_VALUE = "batchExec";

	private static final String ODPS_ENDPOING = "odps.endpoint";
	private static final String ODPS_ACCESSID = "odps.accessId";
	private static final String ODPS_ACCESSKEY = "odps.accessKey";
	private static final String ODPS_ALIYUN_ACCOUNT = "odps.aliyunAccount";
	private static final String PREVIEW_CSV_FILE_PREFIX = "result";
	private static final String PREVIEW_CSV_FILE_SUFFIX = ".csv";

	private static final String SKEW_VALUES = "skew_values";

	private static final AtomicInteger TMP_VIEW_SEQUENCE_ID = new AtomicInteger(0);

	// for rg sample
	private static final String INNER_TABLE_NAME = "__inner__tableName__";
	private static final List<String> RG_SAMPLE_KEYS = new ArrayList<>(Arrays.asList(
		"__inner__projectName__",
		"__inner__jobName__",
		"__inner_zk_quorum__",
		"__inner__rg_sample_data_conf_endpoint__",
		"__inner__rg_sample_data_conf_accessId__",
		"__inner__rg_sample_data_conf_accessKey__",
		"__inner__rg_sample_data_conf_project__"));
	// for sts endpoint
	private static final String INNER_STS_ENDPOINT = "__inner__blink_sts_endpoints__";
	// for mq domain_subGroup
	private static final String INNER_MQ_DOMAIN_SUBGROUP = "__inner__blink_mq_domain_subgroup__";

	/**
	 * Configuration for sql parser.
	 */
	private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
		.setParserFactory(FlinkSqlParserImpl.FACTORY)
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
		FlinkPlannerImpl planner = new FlinkPlannerImpl(FRAMEWORK_CONFIG);
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
	 * @return a map that mapping from registered name to registered function
	 */
	public static Map<String, UserDefinedFunction> registerFunctions(
		TableEnvironment tableEnv,
		List<SqlNodeInfo> sqlNodeInfoList,
		ClassLoader classLoader,
		String userPyLibs) throws Exception {
		Map<String, UserDefinedFunction> registeredFunctions = new HashMap<>();

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

				Object instance = createUserDefinedFunction(classLoader, functionName,
															sqlCreateFunction.getClassName());
				if (instance instanceof TableFunction) {
					TableFunction<?> tableFunction = (TableFunction) instance;
					tableEnv.registerFunction(functionName, tableFunction);
					registeredFunctions.put(functionName, tableFunction);
				} else if (instance instanceof AggregateFunction) {
					AggregateFunction<?, ?> aggregateFunction = (AggregateFunction) instance;
					tableEnv.registerFunction(functionName, aggregateFunction);
					registeredFunctions.put(functionName, aggregateFunction);
				} else if (instance instanceof ScalarFunction) {
					ScalarFunction scalarFunction = (ScalarFunction) instance;
					tableEnv.registerFunction(functionName, scalarFunction);
					registeredFunctions.put(functionName, scalarFunction);
				} else if (instance instanceof UDF) {
					HiveSimpleUDF scalarFunction =
						new HiveSimpleUDF(new HiveFunctionWrapper<>(sqlCreateFunction.getClassName()));
					tableEnv.registerFunction(functionName, scalarFunction);
					registeredFunctions.put(functionName, scalarFunction);
				} else if (instance instanceof GenericUDF) {
					HiveGenericUDF scalarFunction =
						new HiveGenericUDF((new HiveFunctionWrapper<>(sqlCreateFunction.getClassName())));
					tableEnv.registerFunction(functionName, scalarFunction);
					registeredFunctions.put(functionName, scalarFunction);
				} else if (instance instanceof UDAF || instance instanceof GenericUDAFResolver2) {
					HiveUDAFFunction aggregateFunction =
						new HiveUDAFFunction(new HiveFunctionWrapper<>(sqlCreateFunction.getClassName()));
					tableEnv.registerFunction(functionName, aggregateFunction);
					registeredFunctions.put(functionName, aggregateFunction);
				} else if (instance instanceof GenericUDTF) {
					HiveGenericUDTF tableFunction =
						new HiveGenericUDTF(new HiveFunctionWrapper<>(sqlCreateFunction.getClassName()));
					tableEnv.registerFunction(functionName, tableFunction);
					registeredFunctions.put(functionName, tableFunction);
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
			registeredFunctions.putAll(pyUDFs);
		}

		return registeredFunctions;
	}

	private static Object createUserDefinedFunction(ClassLoader classLoader, String funcName,
													String functionDef) throws Exception {
		Object func;
		boolean javaClass = functionDef.contains(".");
		if (javaClass) {
			try {
				func = classLoader.loadClass(functionDef).newInstance();
			} catch (Exception e) {
				throw new RuntimeException(
					TableErrors.INST.sqlCreateUserDefinedFuncError(
						funcName,
						functionDef,
						e.getClass().getCanonicalName() + " : " + e.getMessage()),
					e);
			}
		} else {
			try {
				// try deserialize first
				func = SerializationUtil.deSerializeObject(SerializationUtil.hexString2String(functionDef), classLoader);
			} catch (Exception e) {
				try {
					// It might be a java class without package name
					func = classLoader.loadClass(functionDef).newInstance();
				} catch (Exception ex) {
					throw new RuntimeException(
						TableErrors.INST.sqlCreateUserDefinedFuncError(
							funcName,
							functionDef,
							e.getClass().getCanonicalName() + " : " + e.getMessage()),
						e);
				}
			}
		}
		return func;
	}

	/**
	 * Registers tables to the tableEnvironment.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 */
	public static void registerTables(
		TableEnvironment tableEnv,
		List<SqlNodeInfo> sqlNodeInfoList,
		Properties userParams,
		ClassLoader classLoader) {

		FlinkTableFactory tableFactory = FlinkTableFactory.INSTANCE;
		tableFactory.setClassLoader(classLoader);

		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateTable) {
				final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNodeInfo.getSqlNode();
				final String tableName = sqlCreateTable.getTableName().toString();

				//set with properties
				SqlNodeList propertyList = sqlCreateTable.getPropertyList();
				checkNotNull(propertyList, "Properties of table " + tableName + " is missing");
				TableProperties properties = createTableProperties(propertyList, userParams, tableName);

				//set table environment
				if (tableEnv instanceof StreamTableEnvironment) {
					properties.setString(BLINK_ENVIRONMENT_TYPE_KEY, BLINK_ENVIRONMENT_STREAM_VALUE);
				} else if (tableEnv instanceof BatchTableEnvironment) {
					properties.setString(BLINK_ENVIRONMENT_TYPE_KEY, BLINK_ENVIRONMENT_BATCHEXEC_VALUE);
				}

				RichTableSchema schema = SqlJobUtil.createBlinkTableSchema(sqlCreateTable);

				// create and register
				if ("SOURCE".equals(sqlCreateTable.getTableType())) {
					try {
						TableSource tableSource =
							tableFactory.createTableSource(tableName, schema, properties);

						// register table with origin columns provided by table source
						String tempTable = tableName;
						TableSourceParser parser = tableFactory.createParser(
							tableName, schema, properties);

						List<String> primaryKeys = schema.getPrimaryKeys();
						Boolean hasPks = primaryKeys != null && !primaryKeys.isEmpty();
						Set<Set<String>> primaryKeySet = hasPks ?
							ImmutableSet.of(ImmutableSet.copyOf(primaryKeys)) : null;

						ImmutableSet.Builder<Set<String>> uniqueKeySetBuilder = ImmutableSet.builder();
						List<List<String>> uniqueKeys = schema.getUniqueKeys();
						if (uniqueKeys != null) {
							for (List<String> uniqueKey : uniqueKeys) {
								if (uniqueKey != null && !uniqueKey.isEmpty()) {
									uniqueKeySetBuilder.add(ImmutableSet.copyOf(uniqueKey));
								}
							}
						}
						Set<Set<String>> uniqueKeySet = uniqueKeySetBuilder.build();

						Boolean hasPksOfStream = hasPks && tableEnv instanceof StreamTableEnvironment;

						if (parser != null || sqlCreateTable.containsComputedColumn() ||
							sqlCreateTable.getWatermark() != null || hasPksOfStream) {
							tempTable = tableEnv.createUniqueTableName();
						}
						if (tableEnv instanceof BatchTableEnvironment) {
							Map<String, List<Object>> skewedValues = getSkewedValues(properties);

							((BatchTableEnvironment) tableEnv).registerTableSource(
									tempTable, tableSource, primaryKeySet, skewedValues);
						} else {
							tableEnv.registerTableSource(tempTable, tableSource);
						}

						if (!uniqueKeySet.isEmpty()) {
							tableEnv.alterUniqueKeys(new String[]{tempTable}, uniqueKeySet);
						}

						if (parser != null) {
							TableFunction<?> tableFunction = parser.getParser();
							String tfName = tableFunction.toString();
							String functionName = tableEnv.createUniqueAttributeName(tfName);
							String lateralTableName = tableEnv.createUniqueTableName();
							tableEnv.registerFunction(functionName, tableFunction);
							List<String> parameters = parser.getParameters();
							// t1.`c1`, t2.`c2`
							String projection = lateralTableName + ".*";
							// lateral table(parser(t1.`c1`)) as t2
							String tableFunctionCall =
								"lateral table(" + functionName + "(" +
									tempTable + ".`" + StringUtils.join(parameters, "`, " + tempTable + ".`") + "`)" +
									") as " + lateralTableName;
							String parseSql = "select " + projection + " from " + tempTable + ", " + tableFunctionCall;
							Table viewTable = tableEnv.sqlQuery(String.format(parseSql));
							tempTable = tableName;
							if (sqlCreateTable.containsComputedColumn() || sqlCreateTable.getWatermark() != null
								|| hasPksOfStream) {
								tempTable = tableEnv.createUniqueTableName();
							}

							tableEnv.registerTable(tempTable, viewTable);
						}

						// register table with computed column
						if (sqlCreateTable.containsComputedColumn()) {
							String viewSql = "select " + sqlCreateTable.getColumnSqlString() + " from " + tempTable;
							Table viewTable = tableEnv.sqlQuery(viewSql);
							tempTable = tableName;
							if (sqlCreateTable.getWatermark() != null || hasPksOfStream) {
								tempTable = tableEnv.createUniqueTableName();
							}
							tableEnv.registerTable(tempTable, viewTable);
						}

						// register table with rowtime and watermark
						if (sqlCreateTable.getWatermark() != null) {
							String rowtimeField = sqlCreateTable.getWatermark().getColumnName().toString();
							String tempTable2 = tableName;
							if (hasPksOfStream) {
								tempTable2 = tableEnv.createUniqueTableName();
							}
							((StreamTableEnvironment) tableEnv).registerTableWithWatermark(
								tempTable2,
								tableEnv.scan(tempTable),
								rowtimeField,
								WatermarkUtils.getWithOffsetParameters(
										rowtimeField,
										sqlCreateTable.getWatermark().getFunctionCall()));
							tempTable = tempTable2;
						}

						if (hasPksOfStream) {
							((StreamTableEnvironment) tableEnv).registerTableWithPk(
								tableName,
								tableEnv.scan(tempTable),
								primaryKeys
							);
						}

					} catch (Throwable t) {
						throw new RuntimeException(TableErrors.INST.sqlRegisterTableErrorAsSource(
							tableName, t.getClass().getCanonicalName() + ": " + t.getMessage()),
							t);
					}

				} else if ("SINK".equals(sqlCreateTable.getTableType())) {
					try {
						TableSink tableSink = tableFactory.createTableSink(tableName, schema, properties);
						tableEnv.registerTableSink(
							tableName,
							schema.getColumnNames(),
							schema.getColumnTypes(),
							tableSink);

					} catch (Throwable t) {
						throw new RuntimeException(TableErrors.INST.sqlRegisterTableErrorAsSink(
							tableName, t.getClass().getCanonicalName() + ": " + t.getMessage()),
							t);
					}
				} else if ("DIM".equals(sqlCreateTable.getTableType())) {
					try {
						TableSource dimTable =
							tableFactory.createDimensionTableSource(tableName, schema, properties);
						tableEnv.registerTableSource(tableName, dimTable);
					} catch (Throwable t) {
						throw new RuntimeException(TableErrors.INST.sqlRegisterTableErrorAsDim(
							tableName, t.getClass().getCanonicalName() + ": " + t.getMessage()),
							t);
					}
				}
			}
		}
	}

	private static Map<String, List<Object>> getSkewedValues(
			TableProperties properties) throws java.io.IOException {
		String jsonSkewValues = properties.getString(
				ConfigOptions.key(SKEW_VALUES).noDefaultValue());
		Map skewValues = null;
		if (jsonSkewValues != null) {
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
			skewValues = objectMapper.readValue(jsonSkewValues, Map.class);
		}
		return skewValues;
	}

	public static TableProperties createTableProperties(SqlNodeList propertyList,
														Properties userParams,
														String tableName) {
		TableProperties properties = new TableProperties();
		for (SqlNode sqlNode : propertyList) {
			SqlProperty sqlProperty = (SqlProperty) sqlNode;
			properties.setString(sqlProperty.getKeyString().toLowerCase(), sqlProperty.getValueString());
		}
		String startTime = userParams.getProperty(SOURCE_START_TIME);
		if (!properties.containsKey(START_TIME_KEY)) {
			properties.setString(START_TIME_KEY, startTime == null ? now() : startTime);
			// 用户with属性没有startTime时才去取SOURCE_START_TIME_MS
			String startTimeMs = userParams.getProperty(SOURCE_START_TIME_MS);
			if (!properties.containsKey(START_TIME_MS_KEY) && null != startTimeMs && !startTimeMs.isEmpty()){
				properties.setString(START_TIME_MS_KEY, startTimeMs);
			}
		}

		TimeZone timezone = getUserConfigTimeZone(userParams);
		if (!properties.containsKey(TIME_ZONE_KEY) && null != timezone){
			properties.setString(TIME_ZONE_KEY, timezone.getID());
		}

		if (properties.containsKey(STS_ROLE_ARN)) {
			List<String> stsKeys = new ArrayList<>();
			stsKeys.add(STS_UID);
			stsKeys.add(STS_ACCESSID);
			stsKeys.add(STS_ACCESSKEY);
			stsKeys.add(STS_REGIONID);
			stsKeys.forEach(key -> {
				if (!(STS_REGIONID.equals(key) && properties.containsKey(key))) {
					String value = userParams.getProperty(key);
					if (!key.equalsIgnoreCase(STS_UID)) {
						if (value == null || value.isEmpty()) {
							throw new IllegalArgumentException("Property '" + STS_ROLE_ARN
															+ "' has set, but '" + key + ", not set!");
						}
					} else {
						if (value == null || value.isEmpty()) {
							String roleArn = properties.getString(STS_ROLE_ARN, "");
							pattern.matcher(roleArn);
							Matcher matcher = pattern.matcher(roleArn);
							if (!matcher.find() || matcher.groupCount() != 1) {
								throw new IllegalArgumentException("Property '" + STS_ROLE_ARN
																+ "' has set, but '" + key + ", not set!");
							} else {
								value = matcher.group(1);
							}
						}
					}
					properties.setString(key, value);
				}
			});
		}
		if (userParams.containsKey(BRS_END_POINT)){
			String value = userParams.getProperty(BRS_END_POINT);
			if (null != value && !value.isEmpty()) {
				properties.setString(BRS_END_POINT, value);
			}
		}

		properties.setString(INNER_TABLE_NAME, tableName);

		RG_SAMPLE_KEYS.stream().forEach(rgSampleKey -> {
			if (userParams.getProperty(rgSampleKey) != null) {
				properties.setString(rgSampleKey, userParams.getProperty(rgSampleKey));
			}
		});
		if (userParams.getProperty(INNER_STS_ENDPOINT) != null) {
			properties.setString(INNER_STS_ENDPOINT, userParams.getProperty(INNER_STS_ENDPOINT));
		}
		if (userParams.getProperty(INNER_MQ_DOMAIN_SUBGROUP) != null) {
			properties.setString(INNER_MQ_DOMAIN_SUBGROUP, userParams.getProperty(INNER_MQ_DOMAIN_SUBGROUP));
		}

		return properties;
	}

	/**
	 * Registers views as intermediate table of its sub-query to the tableEnvironment.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 */
	public static void registerViews(TableEnvironment tableEnv, List<SqlNodeInfo> sqlNodeInfoList) {
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlCreateView) {
				String subQuery = ((SqlCreateView) (sqlNodeInfo.getSqlNode())).getSubQuerySql();
				Table viewTable = tableEnv.sqlQuery(subQuery);
				List<String> aliasNames = ((SqlCreateView) (sqlNodeInfo.getSqlNode())).getFieldNames();
				if (!aliasNames.isEmpty()) {
					// currently using sql select for view table column names' definition due to TableAPI's wrong usage of AliasNode.
					// TODO replace to `table.as()` call when addendum fix for FLINK-7942 is done.
					// register a tmp view
					String tmpView = "_tmp_" + ((SqlCreateView) sqlNodeInfo.getSqlNode()).getName() + "_" + getTmpViewSequenceId();
					try {
						tableEnv.registerTable(tmpView, viewTable);
					} catch (Exception e) {
						throw new RuntimeException(TableErrors.INST.sqlRegisterViewErrorDueToViewTblRegExp(tmpView), e);
					}
					String viewSql = "select %s from " + tmpView;
					StringBuilder aliasClause = new StringBuilder();
					List<String> inputFields = viewTable.getRelNode().getRowType().getFieldNames();
					assert aliasNames.size() == inputFields.size();
					// assert is not functional when in online environment
					if (aliasNames.size() != inputFields.size()) {
						throw new RuntimeException(
							TableErrors.INST.sqlRegisterViewErrorFieldsMismatch(
								viewTable.tableName(),
								aliasNames.toString(),
								inputFields.toString()));
					}
					for (int idx = 0; idx < aliasNames.size(); idx++) {
						aliasClause.append("`" + inputFields.get(idx) + "` as `" + aliasNames.get(idx) + "`");
						if (idx < aliasNames.size() - 1) {
							aliasClause.append(", ");
						}
					}
					viewTable = tableEnv.sqlQuery(String.format(viewSql, aliasClause.toString()));
				}
				// register directly
				try {
					tableEnv.registerTable(((SqlCreateView) sqlNodeInfo.getSqlNode()).getName(), viewTable);
				} catch (Exception e) {
					throw new RuntimeException(
						TableErrors.INST.sqlRegisterViewErrorDueToViewTblRegExp(
							((SqlCreateView) sqlNodeInfo.getSqlNode()).getName()), e);
				}
			}
		}
	}

	/**
	 * Analyze table statistics.
	 *
	 * @param tableEnv        the {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList the parsed result of a sql context
	 */
	public static void analyzeTableStats(TableEnvironment tableEnv, List<SqlNodeInfo> sqlNodeInfoList) {
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (sqlNodeInfo.getSqlNode() instanceof SqlAnalyzeTable) {
				SqlAnalyzeTable sqlAnalyzeTable = (SqlAnalyzeTable) sqlNodeInfo.getSqlNode();
				String[] tablePath = sqlAnalyzeTable.getTableName().names.toArray(new String[] {});
				String[] columnNames = getColumnsToAnalyze(sqlAnalyzeTable);
				TableStats tableStats = AnalyzeStatistic.generateTableStats(tableEnv, tablePath, columnNames);
				tableEnv.alterTableStats(tablePath, tableStats);
			}
		}
	}

	private static String[] getColumnsToAnalyze(SqlAnalyzeTable analyzeTable) {
		if (!analyzeTable.isWithColumns()) {
			return new String[] {};
		}
		SqlNodeList columnList = analyzeTable.getColumnList();
		int columnCount = columnList.size();
		// analyze all columns or specified columns.
		if (columnCount == 0) {
			return new String[] {"*"};
		}
		String[] columnNames = new String[columnCount];
		for (int i = 0; i < columnCount; i++) {
			SqlIdentifier column = (SqlIdentifier) columnList.get(i);
			columnNames[i] = column.getSimple();
		}
		return columnNames;
	}

	/**
	 * Do with insert statements: pass to tableEnvironment directly.
	 * Do with select statements: output result if the result directory is not empty.
	 * Do with describe statement: output table info or column info.
	 *  @param isStream                  Is stream or batch
	 * @param tableEnv                   The {@link TableEnvironment} of the sql job
	 * @param sqlNodeInfoList            The parsed result of a sql context
	 * @param previewDir                 The temporary result of the previewing query and dml directory
	 * @param previewCsvNumFiles         The csv file number
	 * @param previewLimit               The limit of preview select, only work for batch
	 * @param previewFieldDelim          The preview field delimiter
	 * @param previewRecordDelim         The preview record delimiter
	 * @param previewQuoteCharacter      The preview quote character
	 * @param previewUseRetractSink      Whether preview use retract csv sink, only work for stream
	 * @param previewMergeRetractResult  Whether merge retract result, only work for stream
	 */
	public static void processOutputStatements(
		boolean isStream,
		TableEnvironment tableEnv,
		List<SqlNodeInfo> sqlNodeInfoList,
		Properties userParams,
		String previewDir,
		int previewCsvNumFiles,
		int previewLimit,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter,
		boolean previewUseRetractSink,
		boolean previewMergeRetractResult) {
		int fileNo = 0;
		TimeZone timezone = getUserConfigTimeZone(userParams);
		for (SqlNodeInfo sqlNodeInfo : sqlNodeInfoList) {
			if (SqlKind.DML.contains(sqlNodeInfo.getSqlNode().getKind())) {
				if (StringUtils.isEmpty(previewDir)) {
					tableEnv.sqlUpdate(sqlNodeInfo.getOriginSql());
				} else {
					// Preview dirs are not empty, output results, dry run
					if (sqlNodeInfo.getSqlNode().getKind().equals(SqlKind.INSERT)
						&& ((SqlInsert) sqlNodeInfo.getSqlNode()).getSource() != null) {
						fileNo++;
						doWithPreviewingQuery(
								isStream,
								((SqlInsert) sqlNodeInfo.getSqlNode()).getSource(),
								tableEnv,
								timezone,
								previewDir,
								previewCsvNumFiles,
								previewLimit,
								previewFieldDelim,
								previewRecordDelim,
								previewQuoteCharacter,
								fileNo,
								previewUseRetractSink,
								previewMergeRetractResult);
					} else if (sqlNodeInfo.getSqlNode().getKind().equals(SqlKind.UPDATE)
						&& ((SqlUpdate) sqlNodeInfo.getSqlNode()).getSourceSelect() != null) {
						fileNo++;
						doWithPreviewingQuery(
								isStream,
								((SqlUpdate) sqlNodeInfo.getSqlNode()).getSourceSelect(),
								tableEnv,
								timezone,
								previewDir,
								previewCsvNumFiles,
								previewLimit,
								previewFieldDelim,
								previewRecordDelim,
								previewQuoteCharacter,
								fileNo,
								previewUseRetractSink,
								previewMergeRetractResult);
					}
				}
			} else if (SqlKind.QUERY.contains(sqlNodeInfo.getSqlNode().getKind())) {
				if (StringUtils.isEmpty(previewDir)) {
					// TODO We may directly output the result to the console later
					// Here we just ignore all select statements when no previewCSVPath is specified
					LOG.warn("Preview directory is empty");
				} else {
					fileNo++;
					doWithPreviewingQuery(
						isStream,
						sqlNodeInfo.getSqlNode(),
						tableEnv,
						timezone,
						previewDir,
						previewCsvNumFiles,
						previewLimit,
						previewFieldDelim,
						previewRecordDelim,
						previewQuoteCharacter,
						fileNo,
						previewUseRetractSink,
						previewMergeRetractResult);
				}
			} else if (sqlNodeInfo.getSqlNode().getKind().equals(SqlKind.DESCRIBE_TABLE)) {
				fileNo++;
				describeTableOrColumn(
						isStream,
						(SqlDescribeTable) sqlNodeInfo.getSqlNode(),
						tableEnv,
						previewDir,
						previewCsvNumFiles,
						previewFieldDelim,
						fileNo,
						timezone);
			}
		}
	}

	private static void describeTableOrColumn(
		boolean isStream,
		SqlDescribeTable describeTable,
		TableEnvironment tableEnv,
		String previewDir,
		int previewCsvNumFiles,
		String previewFieldDelim,
		int fileNo,
		TimeZone timezone) {
		String[] tablePath = describeTable.getTable().names.toArray(new String[] {});
		boolean isRich = describeTable instanceof SqlRichDescribeTable;
		boolean isDescribeColumn = describeTable.getColumn() != null;
		List<Row> results = null;
		String fields = null;
		RowTypeInfo rowTypeInfo = null;
		if (isDescribeColumn) {
			String column = describeTable.getColumn().getSimple();
			results = DescribeTableColumn.describeColumn(tableEnv, tablePath, column, isRich);
			fields = "info_name, info_value";
			rowTypeInfo = new RowTypeInfo(
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO);
		} else {
			results = DescribeTableColumn.describeTable(tableEnv, tablePath, isRich);
			fields = "column_name, column_type, is_nullable";
			rowTypeInfo = new RowTypeInfo(
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.STRING_TYPE_INFO);
		}
		String completePreviewDir = getNormalizedPreviewDir(previewDir, fileNo);
		Table table = null;
		if (!isStream) {
			org.apache.flink.table.api.java.BatchTableEnvironment tEnv =
					(org.apache.flink.table.api.java.BatchTableEnvironment) tableEnv;
			table = tEnv.fromCollection(results, rowTypeInfo, fields);

		}  else {
			StreamExecutionEnvironment execEnv = ((StreamTableEnvironment) tableEnv).execEnv();
			org.apache.flink.table.api.java.StreamTableEnvironment tEnv =
					(org.apache.flink.table.api.java.StreamTableEnvironment) tableEnv;
			table = tEnv.fromDataStream(execEnv.fromCollection(results, rowTypeInfo), fields);
		}
		table.writeToSink(new CsvTableSink(
				completePreviewDir,
				previewFieldDelim,
				previewCsvNumFiles,
				FileSystem.WriteMode.OVERWRITE,
				true,
				timezone));
	}

	private static void doWithPreviewingQuery(
		boolean isStream,
		SqlNode queryNode,
		TableEnvironment tableEnv,
		TimeZone timezone,
		String previewDir,
		int previewCsvNumFiles,
		int previewLimit,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter,
		int fileNo,
		boolean previewUseRetractSink,
		boolean previewMergeRetractResult) {
		String completePreviewDir = getNormalizedPreviewDir(previewDir, fileNo);

		if (!isStream) {
			doBatchPreview(
				queryNode, tableEnv, timezone, completePreviewDir, previewCsvNumFiles, previewLimit,
				previewFieldDelim, previewRecordDelim, previewQuoteCharacter);
		} else {
			doStreamPreview(queryNode, tableEnv, timezone, completePreviewDir, previewCsvNumFiles,
				previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
				previewUseRetractSink, previewMergeRetractResult);
		}
	}

	private static void doBatchPreview(
		SqlNode queryNode,
		TableEnvironment tableEnv,
		TimeZone timezone,
		String previewDir,
		int previewCsvNumFiles,
		int previewLimit,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter) {
		// Add LIMIT to the queryNode
		SqlNumericLiteral limit = SqlLiteral.createExactNumeric(
			String.valueOf(previewLimit),
			SqlParserPos.ZERO);
		SqlOrderBy orderBy;
		if (queryNode instanceof SqlOrderBy) {
			// If it is an ORDER BY, we will write the SQL
			if (((SqlOrderBy) queryNode).fetch != null
				&& ((SqlOrderBy) queryNode).fetch instanceof SqlNumericLiteral
				&& ((SqlNumericLiteral) ((SqlOrderBy) queryNode).fetch).getValueAs(Integer.class)
				<= previewLimit) {
				// If there exists a limit and limit is less than the preview limit, we don't do any
				// rewrite here.
				orderBy = (SqlOrderBy) queryNode;
			} else {
				orderBy = new SqlOrderBy(
					queryNode.getParserPosition(),
					((SqlOrderBy) queryNode).query,
					((SqlOrderBy) queryNode).orderList,
					((SqlOrderBy) queryNode).offset,
					limit);
			}
		} else {
			// instanceof SqlSelect
			orderBy = new SqlOrderBy(
				SqlParserPos.ZERO,
				queryNode,
				SqlNodeList.EMPTY,
				null,
				limit);
		}
		// force parentheses
		String originSql = orderBy.toSqlString(AnsiSqlDialect.DEFAULT, true).getSql();
		Table t = tableEnv.sqlQuery(originSql);
		t.writeToSink(new CsvTableSink(
			previewDir,
			previewFieldDelim,
			previewRecordDelim,
			previewQuoteCharacter,
			previewCsvNumFiles,
			FileSystem.WriteMode.OVERWRITE,
			true,
			timezone));
	}

	private static void doStreamPreview(
		SqlNode queryNode,
		TableEnvironment tableEnv,
		TimeZone timezone,
		String previewDir,
		int previewCsvNumFiles,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter,
		boolean previewUseRetractSink,
		boolean previewMergeRetractResult) {
		// force parentheses
		String originSql = queryNode.toSqlString(AnsiSqlDialect.DEFAULT, true).getSql();
		if (previewUseRetractSink) {
			if (previewMergeRetractResult) {
				Table t = tableEnv.sqlQuery(originSql);
				t.writeToSink(new RetractMergeCsvTableSink(
					previewDir,
					previewFieldDelim,
					previewRecordDelim,
					previewQuoteCharacter,
					previewCsvNumFiles,
					FileSystem.WriteMode.OVERWRITE,
					true,
					timezone
				));
			} else {
				Table t = tableEnv.sqlQuery(originSql);
				t.writeToSink(new RetractCsvTableSink(
					previewDir,
					previewFieldDelim,
					previewRecordDelim,
					previewQuoteCharacter,
					previewCsvNumFiles,
					FileSystem.WriteMode.OVERWRITE,
					true,
					timezone));
			}
		} else {
			Table t = tableEnv.sqlQuery(originSql);
			t.writeToSink(new UpsertCsvTableSink(
				previewDir,
				previewFieldDelim,
				previewRecordDelim,
				previewQuoteCharacter,
				previewCsvNumFiles,
				FileSystem.WriteMode.OVERWRITE,
				true,
				timezone));
		}
	}

	private static String getNormalizedPreviewDir(String previewDir, int fileNo) {
		if (!previewDir.endsWith("/")) {
			previewDir += "/";
		}
		previewDir += (PREVIEW_CSV_FILE_PREFIX + fileNo + PREVIEW_CSV_FILE_SUFFIX);
		return previewDir;
	}

	private static String now() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}

	private static int getTmpViewSequenceId() {
		return TMP_VIEW_SEQUENCE_ID.incrementAndGet();
	}

	public static TimeZone getUserConfigTimeZone(Properties userParams) throws DateTimeException {
		if (userParams != null) {
			String zoneID = userParams.getProperty(ConfConstants.BLINK_JOB_TIMEZONE.toLowerCase());
			if (zoneID == null) {
				zoneID = userParams.getProperty(ConfConstants.BLINK_JOB_TIMEZONE);
			}
			if (!org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(zoneID)) {
				ZoneId zoneId = null;
				try {
					zoneId = ZoneId.of(zoneID.trim());
				} catch (Exception ex) {
					throw new DateTimeException("Invalid TimeZone ID. Please refer to java.util.TimeZone#getAvailableIDs()");
				}
				if (zoneId != null) {
					return TimeZone.getTimeZone(zoneId);
				}
			}
		}

		// default
		return TimeZone.getTimeZone("UTC");
	}
}
