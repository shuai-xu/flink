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

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.table.api.TableEnvironment;

import com.alibaba.blink.launcher.api.JobBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A utility class providing static methods for building sql/tableAPI jobs.
 */
public class JobBuildHelper {

	private static final Logger LOG = LoggerFactory.getLogger(JobBuildHelper.class);

	/**
	 * Build sql job.
	 *
	 * @param isStream                   Is stream or batch
	 * @param sqlFile                    Sql file path
	 * @param userClassLoader            User class loader
	 * @param userParams                 User specific parameters
	 * @param tableEnvironment           The {@link TableEnvironment} of the sql job
	 * @param previewCsvPath             The DFS directory for the result of the previewed query, it can be null
	 *                                   or empty. There will be no preview
	 * @param previewCsvNumFiles         The number of csv file
	 * @param previewLimit               The limit of the preview select, only work for batch
	 * @param previewFieldDelim          The field delimiter of the csv file
	 * @param previewRecordDelim         The record delimiter of the csv file
	 * @param previewQuoteCharacter      The quote character of the csv file
	 * @param previewUseRetractSink      Whether preview use retract csv sink, only work for stream
	 * @param previewMergeRetractResult  Whether merge retract result, only work for stream
	 * @throws Exception                 Exception when build error
	 */
	public static void buildSqlJob(
		boolean isStream,
		String sqlFile,
		ClassLoader userClassLoader,
		Properties userParams,
		String userPyLibs,
		TableEnvironment tableEnvironment,
		String previewCsvPath,
		int previewCsvNumFiles,
		int previewLimit,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter,
		boolean previewUseRetractSink,
		boolean previewMergeRetractResult) throws Exception {
		//parse sql
		String sql = new String(Files.readAllBytes(Paths.get(sqlFile)), StandardCharsets.UTF_8);
		buildSqlJobByString(isStream, userClassLoader, userParams, userPyLibs,
			tableEnvironment, sql, previewCsvPath,
			previewCsvNumFiles, previewLimit,
			previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
			previewUseRetractSink, previewMergeRetractResult);
	}

	/**
	 * Build sql job.
	 *
	 * @param isStream                   Is stream or batch
	 * @param userClassLoader            User class loader
	 * @param userParams                 User specific parameters
	 * @param tableEnvironment           The {@link TableEnvironment} of the sql job
	 * @param sql                        The sql content
	 * @param previewCsvPath             The DFS directory for the result of the previewed query, it can be null
	 *                                   or empty. There will be no preview
	 * @param previewCsvNumFiles         The number of csv file
	 * @param previewLimit               The limit of the preview select, only work for batch
	 * @param previewFieldDelim          The field delimiter of the csv file
	 * @param previewRecordDelim         The record delimiter of the csv file
	 * @param previewQuoteCharacter      The quote character of the csv file
	 * @param previewUseRetractSink      Whether preview use retract csv sink, only work for stream
	 * @param previewMergeRetractResult  Whether merge retract result, only work for stream
	 * @throws Exception                 Exception when build error
	 */
	public static void buildSqlJobByString(
		boolean isStream,
		ClassLoader userClassLoader,
		Properties userParams,
		String userPyLibs,
		TableEnvironment tableEnvironment,
		String sql,
		String previewCsvPath,
		int previewCsvNumFiles,
		int previewLimit,
		String previewFieldDelim,
		String previewRecordDelim,
		String previewQuoteCharacter,
		boolean previewUseRetractSink,
		boolean previewMergeRetractResult) throws Exception {
		List<SqlNodeInfo> sqlNodeInfoList = SqlJobAdapter.parseSqlContext(sql);
		//build sql job
		SqlJobAdapter.registerFunctions(tableEnvironment, sqlNodeInfoList, userClassLoader, userPyLibs);
		SqlJobAdapter.registerTables(tableEnvironment, sqlNodeInfoList, userParams, userClassLoader);
		SqlJobAdapter.registerViews(tableEnvironment, sqlNodeInfoList);
		SqlJobAdapter.analyzeTableStats(tableEnvironment, sqlNodeInfoList);
		SqlJobAdapter.processOutputStatements(
			isStream, tableEnvironment, sqlNodeInfoList, userParams,
			previewCsvPath, previewCsvNumFiles, previewLimit,
			previewFieldDelim, previewRecordDelim, previewQuoteCharacter,
			previewUseRetractSink, previewMergeRetractResult);
	}

	/**
	 * Build sql job.
	 *
	 * @param isStream           Is stream or batch
	 * @param userClassLoader    User class loader
	 * @param userParams         User specific parameters
	 * @param tableEnvironment   The {@link TableEnvironment} of the sql job
	 * @param sql                The sql content
	 * @throws Exception         Exception when build error
	 */
	public static void buildSqlJobByString(
		boolean isStream,
		ClassLoader userClassLoader,
		Properties userParams,
		TableEnvironment tableEnvironment,
		String sql) throws Exception {
		buildSqlJobByString(isStream, userClassLoader, userParams, null,
			tableEnvironment, sql, null, 1, 0,
			null, null, null, false, false);
	}

	/**
	 * Build tableAPI job. It will call the user implemented `build` method.
	 *
	 * @param userClassName      User class name
	 * @param userClassLoader    User class loader
	 * @param userParams         User specific parameters
	 * @param tableEnvironment   Table environment
	 * @throws ClassNotFoundException     User class not found
	 * @throws NoSuchMethodException      The `build` method not found
	 * @throws InvocationTargetException  Call `build` method failed
	 * @throws IllegalAccessException     Illegal access for `build` method
	 */
	public static void callBuild(
		String userClassName,
		ClassLoader userClassLoader,
		Properties userParams,
		TableEnvironment tableEnvironment)
		throws ClassNotFoundException, IllegalAccessException {
		Class<?> cls = userClassLoader.loadClass(userClassName);
		JobBuilder builder = null;
		try {
			builder = (JobBuilder) cls.newInstance();
		} catch (InstantiationException e) {
			LOG.error("User table API class {} could not be instantiated.", userClassName);
		} catch (ClassCastException ce) {
			LOG.error(
				"User table API class have not implements the interface of JobBuilder.",
				userClassName);
		}
		builder.build(tableEnvironment,  userParams == null ? new Properties() : userParams);
	}

	/**
	 * Creates a class loader that contains the mainJar and libJars.
	 *
	 * @param mainJar the main jar file path, can be {@code null}
	 * @param libJars the file paths of libJars
	 * @return a class loader that can find the main jar and lib jars
	 */
	public static ClassLoader createUserClassLoader(String mainJar, String libJars) {
		List<URL> list = new ArrayList<>();

		if (mainJar != null) {
			URL jarFileUrl = getFileURL(mainJar);
			if (jarFileUrl != null) {
				list.add(jarFileUrl);
			}
		}

		if (libJars != null) {
			for (String jar : libJars.split(",")) {
				URL url = getFileURL(jar);
				if (url != null) {
					list.add(url);
				}
			}
		}

		return new URLClassLoader(list.toArray(new URL[list.size()]), Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Adds mainJar and libJars to the given class loader.
	 *
	 * @param mainJar the main jar file path, can be {@code null}
	 * @param libJars the file paths of libJars
	 */
	public static void enhanceClassLoader(ClassLoader classLoader, String mainJar, String libJars) {
		if (classLoader instanceof URLClassLoader) {
			URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
			try {
				Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
				addURLMethod.setAccessible(true);

				if (mainJar != null) {
					URL jarFileUrl = getFileURL(mainJar);
					if (jarFileUrl != null) {
						addURLMethod.invoke(urlClassLoader, jarFileUrl);
					}
				}

				if (libJars != null) {
					for (String jar : libJars.split(",")) {
						URL url = getFileURL(jar);
						if (url != null) {
							addURLMethod.invoke(urlClassLoader, url);
						}
					}
				}
			} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
				LOG.warn("This should not happen!", e);
			}
		} else {
			throw new RuntimeException("The given class loader is not a URLClassLoader.");
		}
	}

	private static URL getFileURL(String filePath) {
		try {
			return Paths.get(filePath).toUri().toURL();
		} catch (MalformedURLException ignored) {
			return null;
		}
	}

	/**
	 * Displays an exception message.
	 *
	 * @param t The exception to display.
	 * @return The return code for the process.
	 */
	public static int handleError(Throwable t) {
		LOG.error("Error while running the command.", t);

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		if (t.getCause() instanceof InvalidProgramException) {
			System.err.println(t.getCause().getMessage());
			StackTraceElement[] trace = t.getCause().getStackTrace();
			for (StackTraceElement ele : trace) {
				System.err.println("\t" + ele.toString());
				String methodName = ele.getMethodName();
				if ("main".equals(methodName)) {
					break;
				}
			}
		} else {
			t.printStackTrace();
		}
		return 1;
	}

	public static void writeFile(File file, String contents) throws IOException {
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
			bw.write(contents);
		}
	}

}
