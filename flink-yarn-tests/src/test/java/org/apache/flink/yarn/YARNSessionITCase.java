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

package org.apache.flink.yarn;

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.TestBaseUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertEquals;
import static org.apache.flink.yarn.util.YarnTestUtils.getTestJarPath;

/**
 * This test starts a MiniYARNCluster with a CapacityScheduler.
 * All function test will be added here, allocate container; YARN rm failover; rm failover; taskmamanager failed, etc.
 */
public class YARNSessionITCase extends YarnTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(YARNSessionITCase.class);

	private static ArrayList<String> appsToIgnore = new ArrayList<>();

	@Rule
	public TestName testName = new TestName();

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-session");
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	/**
	 * Session will allocate all containers from YARN when it start up.
	 * Check job could be submitted successfully.
	 */
	@Test(timeout = 100000)
	public void testAllocateContainerImmediately() throws Exception {
		LOG.info("Starting " + testName.getMethodName());
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-s", "3", // set the slots 3 to check if the vCores are set properly!
				"-nm", "customName",
				"-D" + TaskManagerOptions.TASK_MANAGER_CORE.key() + "=2",
				"-D" + TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + "=128"},
			"Flink JobManager is now running",
			RunTypes.YARN_SESSION);

		// All containers should be launched before job submission
		while (getRunningContainers() < 2) {
			LOG.info("Waiting for all containers to be launched");
			Thread.sleep(500);
		}

		// ------------------------ Test if JobManager web interface is accessible -------

		final YarnClient yc = YarnClient.createYarnClient();
		yc.init(YARN_CONFIGURATION);
		yc.start();

		List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		Assert.assertEquals(1, apps.size()); // Only one running
		ApplicationReport app = apps.get(0);
		Assert.assertEquals("customName", app.getName());
		String url = app.getTrackingUrl();
		if (!url.endsWith("/")) {
			url += "/";
		}
		if (!url.startsWith("http://")) {
			url = "http://" + url;
		}
		LOG.info("Got application URL from YARN {}", url);

		int slotNumber = getSlotNumber(url + "taskmanagers/", 3);
		Assert.assertEquals("unexpected slot number: " + slotNumber, 3, slotNumber);

		// get the configuration from webinterface & check if the dynamic properties from YARN show up there.
		String jsonConfig = TestBaseUtils.getFromHTTP(url + "jobmanager/config");
		Map<String, String> parsedConfig = WebMonitorUtils.fromKeyValueJsonArray(jsonConfig);

		// Check the hostname/port
		String oC = outContent.toString();
		Pattern p = Pattern.compile("Flink JobManager is now running on ([a-zA-Z0-9.-]+):([0-9]+)");
		Matcher matches = p.matcher(oC);
		String hostname = null;
		String port = null;
		while (matches.find()) {
			hostname = matches.group(1).toLowerCase();
			port = matches.group(2);
		}
		LOG.info("Extracted hostname:port: {} {}", hostname, port);

		Assert.assertEquals("unable to find hostname in " + jsonConfig, hostname,
			parsedConfig.get(JobManagerOptions.ADDRESS.key()));

		// test logfile access
		String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
		Assert.assertTrue(logs.contains("Starting rest endpoint"));
		Assert.assertTrue(logs.contains("Starting the SlotManager"));
		Assert.assertTrue(logs.contains("Starting TaskManagers"));

		yc.stop();

		// assert container number
		Assert.assertTrue("Container number should be greater than 2, while actual is " + getRunningContainers(),
			getRunningContainers() >= 2);

		// Check container resource
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			if (containers == null || containers.isEmpty()) {
				continue;
			}
			for (Container container : containers.values()) {
				if (container.getLaunchContext().getCommands().get(0).
						contains(YarnTaskExecutorRunner.class.getSimpleName())) {
					assertEquals(Resource.newInstance(2624, 1), container.getResource());
				}
			}
		}

		// Submit a job and the session has enough resource to execute
		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
		// get temporary file for reading input data for wordcount example
		File tmpInFile = tmp.newFile();
		FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

		Runner jobRunner = startWithArgs(new String[]{"run",
						exampleJarLocation.getAbsolutePath(),
						"--input", tmpInFile.getAbsoluteFile().toString()},
				"Job Runtime: ", RunTypes.CLI_FRONTEND);

		jobRunner.join();

		// send "stop" command to command line interface
		runner.sendStop();
		// wait for the thread to stop
		try {
			runner.join();
		} catch (InterruptedException e) {
			LOG.warn("Interrupted while stopping runner", e);
		}
		LOG.warn("stopped");

		// ----------- Send output to logger
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);
		oC = outContent.toString();
		String eC = errContent.toString();
		LOG.info("Sending stdout content through logger: \n\n{}\n\n", oC);
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", eC);

		LOG.info("Finished " + testName.getMethodName());
	}

	@Test(timeout = 100000)
	public void testAllocateContainerImmediatelyWithResourceSetting() throws Exception {
		LOG.info("Starting " + testName.getMethodName());
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-s", "3", // set the slots 3 to check if the vCores are set properly!
				"-nm", "customName",
				"-D" + TaskManagerOptions.TASK_MANAGER_CORE.key() + "=2",
				"-D" + TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + "=128"},
			"Flink JobManager is now running",
			RunTypes.YARN_SESSION);

		// All containers should be launched before job submission
		while (getRunningContainers() < 2) {
			LOG.info("Waiting for all containers to be launched");
			Thread.sleep(500);
		}

		// ------------------------ Test if JobManager web interface is accessible -------

		final YarnClient yc = YarnClient.createYarnClient();
		yc.init(YARN_CONFIGURATION);
		yc.start();

		List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		Assert.assertEquals(1, apps.size()); // Only one running
		ApplicationReport app = apps.get(0);
		Assert.assertEquals("customName", app.getName());
		String url = app.getTrackingUrl();
		if (!url.endsWith("/")) {
			url += "/";
		}
		if (!url.startsWith("http://")) {
			url = "http://" + url;
		}
		LOG.info("Got application URL from YARN {}", url);

		int slotNumber = getSlotNumber(url + "taskmanagers/", 3);
		Assert.assertEquals("unexpected slot number: " + slotNumber, 3, slotNumber);

		// get the configuration from webinterface & check if the dynamic properties from YARN show up there.
		String jsonConfig = TestBaseUtils.getFromHTTP(url + "jobmanager/config");
		Map<String, String> parsedConfig = WebMonitorUtils.fromKeyValueJsonArray(jsonConfig);

		// Check the hostname/port
		String oC = outContent.toString();
		Pattern p = Pattern.compile("Flink JobManager is now running on ([a-zA-Z0-9.-]+):([0-9]+)");
		Matcher matches = p.matcher(oC);
		String hostname = null;
		String port = null;
		while (matches.find()) {
			hostname = matches.group(1).toLowerCase();
			port = matches.group(2);
		}
		LOG.info("Extracted hostname:port: {} {}", hostname, port);

		Assert.assertEquals("unable to find hostname in " + jsonConfig, hostname,
			parsedConfig.get(JobManagerOptions.ADDRESS.key()));

		// test logfile access
		String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
		Assert.assertTrue(logs.contains("Starting rest endpoint"));
		Assert.assertTrue(logs.contains("Starting the SlotManager"));
		Assert.assertTrue(logs.contains("Starting TaskManagers"));

		yc.stop();

		// assert container number
		Assert.assertTrue("Container number should be greater than 2, while actual is " + getRunningContainers(),
			getRunningContainers() >= 2);

		// Check container resource
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			if (containers == null || containers.isEmpty()) {
				continue;
			}
			for (Container container : containers.values()) {
				if (container.getLaunchContext().getCommands().get(0).
					contains(YarnTaskExecutorRunner.class.getSimpleName())) {
					assertEquals(Resource.newInstance(2624, 1), container.getResource());
				}
			}
		}

		// Submit a job and the session has enough resource to execute
		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
		// get temporary file for reading input data for wordcount example
		File tmpInFile = tmp.newFile();
		FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

		// Previously parameter "--input" of WordCount example don't support setResources API because of bug
		// https://aone.alibaba-inc.com/project/768768/issue/16441721
		Runner jobRunner = startWithArgs(new String[]{"run",
				exampleJarLocation.getAbsolutePath(),
				//"--input", tmpInFile.getAbsoluteFile().toString(),
				"--resource", "vcores:1,memory:100",
				"--parallelism", "2"},
			"Job Runtime: ", RunTypes.CLI_FRONTEND);

		jobRunner.join();

		// send "stop" command to command line interface
		runner.sendStop();
		// wait for the thread to stop
		try {
			runner.join();
		} catch (InterruptedException e) {
			LOG.warn("Interrupted while stopping runner", e);
		}
		LOG.warn("stopped");

		// ----------- Send output to logger
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);
		oC = outContent.toString();
		String eC = errContent.toString();
		LOG.info("Sending stdout content through logger: \n\n{}\n\n", oC);
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", eC);

		LOG.info("Finished " + testName.getMethodName());
	}

	@Test(timeout = 100000)
	public void testAllocateContainerTimeoutWithResourceSetting() throws Exception {
		LOG.info("Starting " + testName.getMethodName());
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-s", "3", // set the slots 3 to check if the vCores are set properly!
				"-nm", "customName",
				"-D" + TaskManagerOptions.TASK_MANAGER_CORE.key() + "=2",
				"-D" + TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + "=128"},
			"Flink JobManager is now running",
			RunTypes.YARN_SESSION);

		// All containers should be launched before job submission
		while (getRunningContainers() < 2) {
			LOG.info("Waiting for all containers to be launched");
			Thread.sleep(500);
		}

		// ------------------------ Test if JobManager web interface is accessible -------

		final YarnClient yc = YarnClient.createYarnClient();
		yc.init(YARN_CONFIGURATION);
		yc.start();

		List<ApplicationReport> apps = yc.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		Assert.assertEquals(1, apps.size()); // Only one running
		ApplicationReport app = apps.get(0);
		Assert.assertEquals("customName", app.getName());

		LOG.info("Set application to ignore: {}", app.getApplicationId());
		appsToIgnore.add(app.getApplicationId().toString());

		String url = app.getTrackingUrl();
		if (!url.endsWith("/")) {
			url += "/";
		}
		if (!url.startsWith("http://")) {
			url = "http://" + url;
		}
		LOG.info("Got application URL from YARN {}", url);

		int slotNumber = getSlotNumber(url + "taskmanagers/", 3);
		Assert.assertEquals("unexpected slot number: " + slotNumber, 3, slotNumber);

		// get the configuration from webinterface & check if the dynamic properties from YARN show up there.
		String jsonConfig = TestBaseUtils.getFromHTTP(url + "jobmanager/config");
		Map<String, String> parsedConfig = WebMonitorUtils.fromKeyValueJsonArray(jsonConfig);

		// Check the hostname/port
		String oC = outContent.toString();
		Pattern p = Pattern.compile("Flink JobManager is now running on ([a-zA-Z0-9.-]+):([0-9]+)");
		Matcher matches = p.matcher(oC);
		String hostname = null;
		String port = null;
		while (matches.find()) {
			hostname = matches.group(1).toLowerCase();
			port = matches.group(2);
		}
		LOG.info("Extracted hostname:port: {} {}", hostname, port);

		Assert.assertEquals("unable to find hostname in " + jsonConfig, hostname,
			parsedConfig.get(JobManagerOptions.ADDRESS.key()));

		// test logfile access
		String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
		Assert.assertTrue(logs.contains("Starting rest endpoint"));
		Assert.assertTrue(logs.contains("Starting the SlotManager"));
		Assert.assertTrue(logs.contains("Starting TaskManagers"));

		yc.stop();

		// assert container number
		Assert.assertTrue("Container number should be greater than 2, while actual is " + getRunningContainers(),
			getRunningContainers() >= 2);

		// Check container resource
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			if (containers == null || containers.isEmpty()) {
				continue;
			}
			for (Container container : containers.values()) {
				if (container.getLaunchContext().getCommands().get(0).
					contains(YarnTaskExecutorRunner.class.getSimpleName())) {
					assertEquals(Resource.newInstance(2624, 1), container.getResource());
				}
			}
		}

		// Submit a job and the session has enough resource to execute
		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
		// get temporary file for reading input data for wordcount example
		File tmpInFile = tmp.newFile();
		FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

		// Previously parameter "--input" of WordCount example don't support setResources API because of bug
		// https://aone.alibaba-inc.com/project/768768/issue/16441721
		try {
			Runner jobRunner = startWithArgs(new String[]{"run",
					exampleJarLocation.getAbsolutePath(),
					//"--input", tmpInFile.getAbsoluteFile().toString(),
					"--resource", "vcores:1,memory:100",
					"--parallelism", "3"},
				"Job Runtime: ", RunTypes.CLI_FRONTEND, 20, true);
			jobRunner.join();
			Assert.assertTrue("Expect timeout exception", false);
		} catch (RuntimeException e) {
			Assert.assertTrue(e.getMessage().startsWith("Expected timeout for"));
		}

		// send "stop" command to command line interface
		runner.sendStop();
		// wait for the thread to stop
		try {
			runner.join();
		} catch (InterruptedException e) {
			LOG.warn("Interrupted while stopping runner", e);
		}
		LOG.warn("stopped");

		// ----------- Send output to logger
		System.setOut(ORIGINAL_STDOUT);
		System.setErr(ORIGINAL_STDERR);
		oC = outContent.toString();
		String eC = errContent.toString();
		LOG.info("Sending stdout content through logger: \n\n{}\n\n", oC);
		LOG.info("Sending stderr content through logger: \n\n{}\n\n", eC);

		LOG.info("Finished " + testName.getMethodName());
	}

	@After
	public void checkForProhibitedLogContents() {
		if (!testName.getMethodName().equals("testAllocateContainerTimeoutWithResourceSetting")) {
			ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS,
				appsToIgnore.toArray(new String[appsToIgnore.size()]));
		}
	}

	private int getSlotNumber(String url, int expect) throws Exception {
		ArrayNode taskManagers = null;
		int slotNumber = 0;
		int index = 0;
		while (taskManagers == null || taskManagers.size() < 1 || index < 60) {
			String response = TestBaseUtils.getFromHTTP(url);
			JsonNode parsedTMs = new ObjectMapper().readTree(response);
			taskManagers = (ArrayNode) parsedTMs.get("taskmanagers");
			Assert.assertNotNull(taskManagers);
			if (taskManagers.size() == 1) {
				slotNumber = taskManagers.get(0).get("slotsNumber").asInt();
				if (slotNumber == expect) {
					break;
				}
			}
			Thread.sleep(500);
			index++;
		}
		return slotNumber;
	}
}
