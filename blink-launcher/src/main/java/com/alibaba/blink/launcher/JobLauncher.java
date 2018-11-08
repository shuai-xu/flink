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

package com.alibaba.blink.launcher;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.functions.python.PythonUDFUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import com.alibaba.blink.launcher.autoconfig.StreamGraphConfigurer;
import com.alibaba.blink.launcher.autoconfig.StreamGraphProperty;
import com.alibaba.blink.launcher.autoconfig.UnexpectedConfigurationException;
import com.alibaba.blink.launcher.autoconfig.errorcode.AutoConfigErrors;
import com.alibaba.blink.launcher.autoconfig.rulebased.RuleBasedStreamGraphPropertyGenerator;
import com.alibaba.blink.launcher.util.EnvUtil;
import com.alibaba.blink.launcher.util.JobBuildHelper;
import com.alibaba.blink.launcher.util.NumUtil;
import com.alibaba.blink.launcher.util.PropertiesUtil;
import com.alibaba.blink.launcher.util.SqlJobAdapter;
import com.alibaba.blink.launcher.util.StreamExecEnvUtil;
import com.alibaba.blink.launcher.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

/**
 * The entry class for submitting a sql/tableAPI job.
 */
public class JobLauncher {

	private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class);

	private static final int DEFAULT_PARALLELISM = 1;
	private static final String ODPS_TAG = "odps";
	private static final String TABLE_TYPE = "table.type";
	private static final String TEMPFILE_ENV = "java.io.tmpdir";
	private static final int DEFAULT_PREVIEW_LIMIT = 100000;
	private static final int DEFAULT_PREVIEW_CSV_NUM_FILES = 1;
	private static final String DEFAULT_PREVIEW_FIELD_DELIM = ",";
	private static final String DEFAULT_PREVIEW_RECORD_DELIM = "\n";
	private static final String DEFAULT_PREVIEW_QUOTE_CHARACTER = "\"";

	public static void main(String[] args) throws Exception {
		System.setProperty("saffron.default.charset", "UTF-16LE");
		System.setProperty("saffron.default.nationalcharset", "UTF-16LE");
		System.setProperty("saffron.default.collation.name", "UTF-16LE$en_US");
		try {
			//parse args
			int i = 0;
			final String action = args[i++]; //run or info
			final boolean isBatchNotStream = "batch".equals(args[i++]); //batch or stream
			final String type = args[i++]; //sql or table
			String sqlFile = null;
			String buildClass = null;
			String libJars = null;
			String usrPyLibs = null;
			String jobName = null;
			String jsonFile = null;
			String outputFile = null;
			String previewCsvPath = null;
			int previewCsvNumFiles = DEFAULT_PREVIEW_CSV_NUM_FILES;
			int previewLimit = DEFAULT_PREVIEW_LIMIT;
			String previewFieldDelim = DEFAULT_PREVIEW_FIELD_DELIM;
			String previewRecordDelim = DEFAULT_PREVIEW_RECORD_DELIM;
			String previewQuoteCharacter = DEFAULT_PREVIEW_QUOTE_CHARACTER;
			boolean previewUseRetractSink = false;
			boolean previewMergeRetractResult = false;

			Properties jobConf = new Properties();
			if ("sql".equals(type)) {
				sqlFile = args[i++];
			} else if ("table".equals(type)) {
				buildClass = args[i++];
			}
			while (i < args.length - 1) {
				switch (args[i++]) {
					case "-addons":
						libJars = args[i++];
						break;
					case "-pyaddons":
						usrPyLibs = args[i++];
						break;
					case "-name":
						jobName = args[i++];

						EnvUtil.addAttrForLogAppender(
								EnvUtil.AttrForAppender.JOB_NAME_ATTR,
								jobName);
						break;
					case "-conf":
						try (InputStream inputStream = new FileInputStream(args[i++])) {
							PropertiesUtil.loadWithTrimmedValues(inputStream, jobConf);
							for (String key : jobConf.stringPropertyNames()) {
								LOG.info("Load user parameter: {}={}", key, jobConf.getProperty(key));
							}
						} catch (IOException ignored) {
						}

						EnvUtil.addAttrForLogAppender(
								EnvUtil.AttrForAppender.CLUSTER_NAME_ATTR,
								jobConf.getProperty(EnvUtil.AttrForAppender.CLUSTER_NAME_ATTR.getName()));
						EnvUtil.addAttrForLogAppender(
								EnvUtil.AttrForAppender.QUEUE_NAME_ATTR,
								jobConf.getProperty(EnvUtil.AttrForAppender.QUEUE_NAME_ATTR.getName()));
						EnvUtil.addAttrForLogAppender(
								EnvUtil.AttrForAppender.APPENDER_LEVEL_ATTR,
								jobConf.getProperty(EnvUtil.AttrForAppender.APPENDER_LEVEL_ATTR.getName()));
						break;
					case "-plan":
						jsonFile = args[i++];
						break;
					case "-output":
						outputFile = args[i++];
						break;
					case "-previewCsvPath":
						previewCsvPath = args[i++];
						break;
					case "-previewCsvNumFiles":
						previewCsvNumFiles = NumUtil.parseInt(args[i++], DEFAULT_PREVIEW_CSV_NUM_FILES);
						break;
					case "-previewLimit":
						previewLimit = NumUtil.parseInt(args[i++]);
						break;
					case "-previewFieldDelim":
						previewFieldDelim = args[i++];
						break;
					case "-previewRecordDelim":
						previewRecordDelim = args[i++];
						break;
					case "-previewQuoteCharacter":
						previewQuoteCharacter = args[i++];
						break;
					case "-previewUseRetractSink":
						previewUseRetractSink = Boolean.parseBoolean(args[i++]);
						break;
					case "-previewMergeRetractResult":
						previewMergeRetractResult = Boolean.parseBoolean(args[i++]);
						break;
					default:
						//ignore
				}
			}

			//run job
			TableConfig conf = new TableConfig();
			setTableConf(conf, jobConf);

			ClassLoader currentClassLoader = JobLauncher.class.getClassLoader();
			if (libJars != null) {
				JobBuildHelper.enhanceClassLoader(currentClassLoader, null, libJars);
			}
			String jobInfo;
			if (isBatchNotStream) {
				jobInfo = runBatch(jobConf, conf, jobName, type, sqlFile, currentClassLoader,
						buildClass, action, jsonFile, usrPyLibs, previewCsvPath, previewCsvNumFiles, previewLimit,
						previewFieldDelim, previewRecordDelim, previewQuoteCharacter);
			} else {
				jobInfo = runStream(jobConf, conf, jobName, type, sqlFile, currentClassLoader,
						buildClass, action, jsonFile, usrPyLibs,
						previewCsvPath, previewCsvNumFiles,
						previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
						previewUseRetractSink, previewMergeRetractResult);
			}
			if (jobInfo != null) {
				if (outputFile != null) {
					JobBuildHelper.writeFile(new File(outputFile), jobInfo);
				} else {
					System.out.println("---PLAN-START---");
					System.out.println(jobInfo);
					System.out.println("---PLAN-END-----");
				}
			}

			// avoid cannot exit because of non deamon thread
			if ("info".equals(action)) {
				System.exit(0);
			}
		} catch (Throwable t) {
			JobBuildHelper.handleError(t);
			throw t;
		}
	}

	/**
	 * Run as batch.
	 *
	 * @param jobConf               The JobConf properties
	 * @param conf                  TableConfig
	 * @param jobName               The job name
	 * @param type                  The job type "sql" or "table"
	 * @param sqlFile               The path of the sql file
	 * @param currentClassLoader    The current class loader
	 * @param buildClass            The build class
	 * @param action                The action "run" or "info"
	 * @param previewCsvDir         The DFS directory for the result of the previewed query, it can
	 *                              be null
	 *                              or empty. There will be no preview
	 * @param previewCsvNumFiles    The number of csv file
	 * @param previewLimit          The limit of the preview select
	 * @param previewFieldDelim     The field delimiter of the csv file
	 * @param previewRecordDelim    The record delimiter of the csv file
	 * @param previewQuoteCharacter The quote character of the csv file
	 * @return jobInfo if has (if the action is "info"), otherwise null.
	 */
	private static synchronized String runBatch(
			Properties jobConf,
			TableConfig conf,
			String jobName,
			String type,
			String sqlFile,
			ClassLoader currentClassLoader,
			String buildClass,
			String action,
			String jsonFilePath,
			String userPyLibs,
			String previewCsvDir,
			int previewCsvNumFiles,
			int previewLimit,
			String previewFieldDelim,
			String previewRecordDelim,
			String previewQuoteCharacter) throws Exception {
		//add user config
		addUserConfig(conf, jobConf);
		if (conf.getOperatorMetricCollect()) {
			// if jobName is defined, set prefix of metric file name to jobName
			String absolutePathOfMetricFile = generateAbsoluteFilePath(jobName, ".metric", conf.getDumpFileOfPlanWithMetrics());
			conf.setDumpFileOfPlanWithMetrics(absolutePathOfMetricFile);
			LOG.info("Set path of plan with metrics to {}", absolutePathOfMetricFile);
		}
		if (conf.getOptimizedPlanCollect()) {
			//  if jobName is defined, set prefix of optimized plan file name to jobName
			String absolutePathOfOptimizedPlanFile = generateAbsoluteFilePath(jobName, ".plan", conf.getDumpFileOfOptimizedPlan());
			conf.setDumpFileOfOptimizedPlan(absolutePathOfOptimizedPlanFile);
			LOG.info("Set path of optimized plan to {}", absolutePathOfOptimizedPlanFile);
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env, conf);

		if ("sql".equals(type)) {
			JobBuildHelper.buildSqlJob(false, sqlFile, currentClassLoader, jobConf, userPyLibs, tEnv,
					previewCsvDir, previewCsvNumFiles, previewLimit,
					previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
					false, false);
		} else if ("table".equals(type)) {
			JobBuildHelper.callBuild(buildClass, currentClassLoader, jobConf, tEnv);
		}

		long s1 = System.currentTimeMillis();
		tEnv.compile();
		long s2 = System.currentTimeMillis();
		LOG.info("compile used time: {} ms", s2 - s1);
		StreamGraph streamGraph = tEnv.generateStreamGraph();
		String jobInfo = execute(env, streamGraph, jobName, jobConf, action, jsonFilePath);
		return jobInfo;
	}

	/**
	 * Run as stream.
	 *
	 * @param jobConf                   The JobConf properties
	 * @param conf                      TableConfig
	 * @param jobName                   The job name
	 * @param type                      The job type "sql" or "table"
	 * @param sqlFilePath               The path of the sql file
	 * @param currentClassLoader        The current class loader
	 * @param buildClass                The build class
	 * @param action                    The action "run" or "info"
	 * @param jsonFilePath              The json file
	 * @param previewCsvDir             The DFS directory for the result of the previewed query, it
	 *                                  can be null
	 *                                  or empty. There will be no preview
	 * @param previewCsvNumFiles        The number of csv file
	 * @param previewFieldDelim         The field delimiter of the csv file
	 * @param previewRecordDelim        The record delimiter of the csv file
	 * @param previewQuoteCharacter     The quote character of the csv file
	 * @param previewUseRetractSink     Whether preview use retract csv sink
	 * @param previewMergeRetractResult Whether merge retract result
	 * @return jobInfo if has (if the action is "info"), otherwise null.
	 */
	public static synchronized String runStream(
			Properties jobConf,
			TableConfig conf,
			String jobName,
			String type,
			String sqlFilePath,
			ClassLoader currentClassLoader,
			String buildClass,
			String action,
			String jsonFilePath,
			String userPyLibs,
			String previewCsvDir,
			int previewCsvNumFiles,
			String previewFieldDelim,
			String previewRecordDelim,
			String previewQuoteCharacter,
			boolean previewUseRetractSink,
			boolean previewMergeRetractResult) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		registerPythonLibFiles(env, jobConf, userPyLibs);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env, conf);
		addUserConfig(conf, jobConf);
		StreamExecEnvUtil.setStreamEnvConfigs(env, jobConf);
		StreamExecEnvUtil.setConfig(env, jobConf);

		long start = System.currentTimeMillis();
		if ("sql".equals(type)) {
			JobBuildHelper.buildSqlJob(
					true, sqlFilePath, currentClassLoader, jobConf, userPyLibs, tEnv, previewCsvDir,
					previewCsvNumFiles, DEFAULT_PREVIEW_LIMIT,
					previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
					previewUseRetractSink, previewMergeRetractResult);
		} else if ("table".equals(type)) {
			JobBuildHelper.callBuild(buildClass, currentClassLoader, jobConf, tEnv);
		}
		long end = System.currentTimeMillis();
		LOG.info("build type {}, used time: {} ms", type, end - start);

		tEnv.compile();
		StreamGraph streamGraph = env.getStreamGraph();
		String jobInfo = execute(env, streamGraph, jobName, jobConf, action, jsonFilePath);
		return jobInfo;
	}

	private static String execute(StreamExecutionEnvironment env, StreamGraph streamGraph,
			String jobName, Properties jobConf, String action, String jsonFilePath) throws Exception {

		String jobInfo = null;

		if ("run".equals(action)) {
			StreamGraphProperty property = loadStreamGraphProperty(jsonFilePath);

			if (property == null) {
				property = new RuleBasedStreamGraphPropertyGenerator.Builder()
						.setUserProperties(jobConf).build().generateProperties(streamGraph);
			}

			StreamGraphConfigurer.configure(streamGraph, property);
			streamGraph.setJobName(jobName);
			JobSubmissionResult result = env.execute(streamGraph);

			logAndSysout(String.format("Submitted job %s to flink cluster.", result.getJobID()));

		} else if ("info".equals(action)) {
			long s1 = System.currentTimeMillis();

			StreamGraphProperty oldProperty = loadStreamGraphProperty(jsonFilePath);

			// generate new property with based on given property.
			StreamGraphProperty property = new RuleBasedStreamGraphPropertyGenerator.Builder()
					.setUserProperties(jobConf).build().generateProperties(streamGraph, oldProperty);
			jobInfo = property.toString();
			long s2 = System.currentTimeMillis();
			LOG.info("compile used time: {} ms", s2 - s1);

		}
		return jobInfo;
	}

	private static StreamGraphProperty loadStreamGraphProperty(
			String jsonFilePath) throws IOException {
		StreamGraphProperty oldProperty = null;
		if (!StringUtils.isNullOrWhitespaceOnly(jsonFilePath)) {
			try {
				String configJson = FileUtils.readFileUtf8(new File(jsonFilePath));
				oldProperty = StreamGraphProperty.fromJson(configJson);
				LOG.info("Parsing json config file as JobConfiguration");
			} catch (IllegalArgumentException e) {
				LOG.warn("Failed parsing json config file as JobConfiguration. {}", e);
				LOG.info("Parsing json config file as ResourceFile");
				throw new UnexpectedConfigurationException(
						AutoConfigErrors.INST.cliAutoConfTransformationCfgSetError(
								"Fail to parse resource configuration file."), e);
			}
		}
		return oldProperty;
	}

	public static void setTableConf(TableConfig conf, Properties userParams) {
		conf.setSubsectionOptimization(true);

		String joinReorder = userParams.getProperty(ConfConstants.BLINK_JOINREORDER_ENABLED);
		if ("true".equalsIgnoreCase(joinReorder)) {
			conf.setJoinReorderEnabled(true);
			StreamExecEnvUtil.log(ConfConstants.BLINK_JOINREORDER_ENABLED, "true");
		}

		String codeGenDebug = userParams.getProperty(ConfConstants.BLINK_CODEGEN_DEBUG);
		if ("true".equalsIgnoreCase(codeGenDebug)) {
			conf.enableCodeGenerateDebug();
			conf.setCodeGenerateTmpDir(".");
			StreamExecEnvUtil.log(ConfConstants.BLINK_CODEGEN_DEBUG, "true");
		}

		String codegenRewite = userParams.getProperty(ConfConstants.BLINK_CODEGEN_REWRITE);
		if ("true".equalsIgnoreCase(codegenRewite)) {
			conf.setCodegenRewriteEnabled(true);
			StreamExecEnvUtil.log(ConfConstants.BLINK_CODEGEN_REWRITE, "true");
		}

		TimeZone timezone = SqlJobAdapter.getUserConfigTimeZone(userParams);
		if (timezone != null) {
			conf.setTimeZone(timezone);
		}
	}

	private static void addUserConfig(TableConfig tableConfig, Properties userParams) {
		//add user config
		for (Object str : userParams.keySet()) {
			String keyStr = String.valueOf(str);
			tableConfig.getParameters().setString(keyStr, userParams.getProperty(keyStr));
		}
	}

	private static String generateAbsoluteFilePath(String prefix, String suffix, String directory) {
		String parentPath = null;
		if (directory != null) {
			parentPath = directory;
		} else {
			// set parent path to temp directory
			parentPath = System.getProperty(TEMPFILE_ENV);
		}
		if (parentPath.endsWith(File.separator)) {
			parentPath = parentPath.substring(
					0, parentPath.length() - File.separator.length());
		}
		String fileName = null;
		if (prefix != null) {
			fileName = prefix + suffix;
		} else {
			// generate unique file name
			fileName = UUID.randomUUID().toString() + suffix;
		}
		return parentPath + File.separator + fileName;
	}

	public static void registerPythonLibFiles(StreamExecutionEnvironment env, Properties jobConf,
			String userPyLibs)
			throws IOException {

		if (StringUtils.isNullOrWhitespaceOnly(userPyLibs)) {
			return;
		}

		// Now, save the keys of registered Distributed Cached Files to jobConf.
		String dcFileKeys = "";
		for (String file : userPyLibs.split(",")) {
			Path filePath = new Path(file);
			if (!FileSystem.getUnguardedFileSystem(filePath.toUri()).exists(filePath)) {
				throw new FileNotFoundException("User's python lib file " + file + " does not exist.");
			}

			// keys should be consistent with JobGraph.addUserArtifact
			URI uri = filePath.toUri();
			final String fileKey = uri.getFragment() != null ? uri.getFragment() : new Path(uri).getName();
			dcFileKeys = dcFileKeys.length() == 0 ? fileKey : (dcFileKeys + "," + fileKey);

			// Bayes will uploads files and Runtime will register to
			// distributed cache via CliFrontend's --files option.
			// but Runtime won't register cached file for local environment now
			// register it for local unit test
			// See: RestclusterClient#uploadUserArtifacts
			if (env instanceof LocalStreamEnvironment) {
				env.registerCachedFile(filePath.getPath(), fileKey);
			}
		}
		if (!StringUtil.isEmpty(dcFileKeys)) {
			jobConf.setProperty(PythonUDFUtil.PYFLINK_CACHED_USR_LIB_IDS, dcFileKeys);
		}
	}

	private static void logAndSysout(String content) {
		LOG.info(content);
		System.out.println(content);
	}
}
