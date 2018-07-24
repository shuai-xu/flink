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
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

	@BeforeClass
	public static void setup() {
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-session");
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	/**
	 * Session will allocate all containers from YARN when it start up.
	 * Check job could be submitted successfully.
	 */
	@Test(timeout = 100000)
	public void testAllocateContainerImmediately() throws Exception {
		LOG.info("Starting testAllocateContainerImmediately()");
		Runner runner = startWithArgs(new String[]{"-j", flinkUberjar.getAbsolutePath(), "-t", flinkLibFolder.getAbsolutePath(),
				"-n", "1",
				"-jm", "768",
				"-tm", "1024",
				"-s", "3", // set the slots 3 to check if the vCores are set properly!
				"-nm", "customName",
				"-D" + YarnConfigOptions.YARN_CLUSTER_TM_CORE.key() + "=2",
				"-D" + YarnConfigOptions.YARN_CLUSTER_TM_EXTENDED_RESOURCES.key() + "=MANAGED_MEMORY_MB:128"},
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

		ArrayNode taskManagers = null;
		while (taskManagers == null || taskManagers.size() < 1) {
			String response = TestBaseUtils.getFromHTTP(url + "taskmanagers/");
			JsonNode parsedTMs = new ObjectMapper().readTree(response);
			taskManagers = (ArrayNode) parsedTMs.get("taskmanagers");
			Assert.assertNotNull(taskManagers);
			Thread.sleep(500);
		}
		Assert.assertEquals(3, taskManagers.get(0).get("slotsNumber").asInt());

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
		/*Assert.assertEquals("unable to find port in " + jsonConfig, port,
			parsedConfig.get(JobManagerOptions.PORT.key()));*/

		// test logfile access
		String logs = TestBaseUtils.getFromHTTP(url + "jobmanager/log");
		Assert.assertTrue(logs.contains("Starting rest endpoint"));
		Assert.assertTrue(logs.contains("Starting the SlotManager"));
		Assert.assertTrue(logs.contains("Starting TaskManagers"));

		yc.stop();

		// Check container resource
		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			if (containers == null || containers.isEmpty()) {
				return;
			}
			for (Container container : containers.values()) {
				if (container.getLaunchContext().getCommands().get(0).
						contains(YarnTaskExecutorRunner.class.getSimpleName())) {
					assertEquals(Resource.newInstance(1792, 1), container.getResource());
				}
			}
		}

		// Submit a job and the session has enough resource to execute
		File exampleJarLocation = getTestJarPath("StreamingWordCount.jar");
		// get temporary file for reading input data for wordcount example
		File tmpInFile = tmp.newFile();
		FileUtils.writeStringToFile(tmpInFile, WordCountData.TEXT);

		Runner jobRunner = startWithArgs(new String[]{"run",
						"--detached", exampleJarLocation.getAbsolutePath(),
						"--input", tmpInFile.getAbsoluteFile().toString()},
				"Job has been submitted with JobID", RunTypes.CLI_FRONTEND);

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

		LOG.info("Finished testAllocateContainerImmediately()");
	}

	@After
	public void checkForProhibitedLogContents() {
		ensureNoProhibitedStringInLogFiles(PROHIBITED_STRINGS, WHITELISTED_STRINGS);
	}
}
