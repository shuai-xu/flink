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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import org.apache.flink.table.api.TableConfig;

import com.alibaba.blink.launcher.JobLauncher;
import com.alibaba.blink.launcher.TestUtil;
import com.alibaba.blink.launcher.autoconfig.UnexpectedConfigurationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


import static com.alibaba.blink.launcher.TestUtil.getResourceContent;
import static com.alibaba.blink.launcher.TestUtil.getResourcePath;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for JobLauncher.
 */
public class RuleBasedConfigurationITTest {
	private static final String SQL = "sql";
	private static final String INFO = "info";
	private static final String RUN = "run";

	// SQL files
	private static final String JOB_CONFIG_FILE = "autoconf/job.config";
	private static final String JOB1_MINIBATCH_CONFIG_FILE = "autoconf/job1-minibatch.config";
	private static final String JOB1_MINIBATCH_PREFER_JSON_CONFIG_FILE = "autoconf/job1-minibatch-prefer-json.config";
	private static final String JOB1_SQL_FILE = "autoconf/job1.sql";
	private static final String JOB3_SQL_FILE = "autoconf/job3.sql";

	private enum TestCase {
		JOBGRAPH_CHANGED,	// Should detect job graph changed
		INVALID_OLD_PLAN,	// Invalid old plan
		NEW,			// When a new job starts with autoconf, and doesn't have old plan
		MINIBATCH_NEW, // A new job with autoconf, enable minibatch
		MINIBATCH_REUSE, // Reuse minibatch conf in old plan
		NATIVEMEMORY,	// Emphasis on native memory
	}

	@Rule
	public ExpectedException expectedEx = ExpectedException.none();

	@Test
	public void testSqlInfo_JOBGRAPH_CHANGED() throws Exception {
		testRunStream(JOB_CONFIG_FILE, SQL, JOB1_SQL_FILE, INFO, getJobConfPath(TestCase.JOBGRAPH_CHANGED), getExpectedPlanPath(TestCase.JOBGRAPH_CHANGED));
	}

	@Test
	public void testSqlInfo_NEW() throws Exception {
		testRunStream(JOB_CONFIG_FILE, SQL, JOB1_SQL_FILE, INFO, null, getExpectedPlanPath(TestCase.NEW));
	}

	@Test
	public void testSqlInfo_MINIBATCH_NEW() throws Exception {
		testRunStream(JOB1_MINIBATCH_CONFIG_FILE, SQL, JOB1_SQL_FILE, INFO, null, getExpectedPlanPath(TestCase.MINIBATCH_NEW));
	}

	@Test
	public void testSqlInfo_MINIBATCH_REUSE() throws Exception {
		testRunStream(JOB1_MINIBATCH_PREFER_JSON_CONFIG_FILE, SQL, JOB1_SQL_FILE, INFO, getJobConfPath(TestCase.MINIBATCH_REUSE), getExpectedPlanPath(TestCase.MINIBATCH_REUSE));
	}

	@Test
	public void testSqlInfo_NATIVEMEMORY() throws Exception {
		testRunStream(JOB_CONFIG_FILE, SQL, JOB3_SQL_FILE, INFO, getJobConfPath(TestCase.NATIVEMEMORY), getExpectedPlanPath(TestCase.NATIVEMEMORY));
	}

	@Test(expected = UnexpectedConfigurationException.class)
	public void testSqlInfo_INVALID_OLDPLAN() throws Exception {
		testRunStream(JOB_CONFIG_FILE, SQL, JOB3_SQL_FILE, RUN, getJobConfPath(TestCase.INVALID_OLD_PLAN), getExpectedPlanPath(TestCase.INVALID_OLD_PLAN));
	}

	private void testRunStream(
		String jobConfigFile,
		String type,
		String sqlFile,
		String action,
		String oldPlanFile,
		String expectPlanFile) throws Exception {

		testRunStreamWithOldPlan(jobConfigFile, type, sqlFile, action, oldPlanFile, getResourceContent(expectPlanFile));
	}

	private void testRunStreamWithOldPlan(
		String jobConfigFile,
		String type,
		String sqlFile,
		String action,
		String oldPlanFile,
		String expectPlan) throws Exception {

		ClassLoader currentClassLoader = JobLauncher.class.getClassLoader();
		TableConfig conf = new TableConfig();
		Properties jobConf = loadProperties(getResourcePath(jobConfigFile));

		JobLauncher.setTableConf(conf, jobConf);

		TestUtil.resetClassStaticFields();
		String actualPlan = JobLauncher.runStream(
			jobConf,
			conf,
			"test",
			type,
			getResourcePath(sqlFile),
			currentClassLoader,
			null,
			action,
			oldPlanFile != null ? getResourcePath(oldPlanFile) : null,
			null,
			null,
			1,
			null,
			null,
			null,
			false,
			false);

		TestUtil.resetClassStaticFields();
		assertEquals(expectPlan, actualPlan + "\n");
	}

	private static String getJobConfPath(TestCase testCase) {
		return String.format("autoconf/%s/jobconf.json", testCase);
	}

	private static String getExpectedPlanPath(TestCase testCase) {
		return String.format("autoconf/%s/expected_plan.json", testCase);
	}

	private static Properties loadProperties(String jobConfFile) {
		Properties jobConf = new Properties();

		try (InputStream inputStream = new FileInputStream(jobConfFile)) {
			jobConf.load(inputStream);
		} catch (IOException ignored) {
		}

		return jobConf;
	}

}
