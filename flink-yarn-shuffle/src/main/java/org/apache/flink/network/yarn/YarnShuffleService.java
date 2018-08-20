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

package org.apache.flink.network.yarn;

import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleService;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;


/**
 * Yarn implementation of flink external shuffle service. It should be deployed along with
 * Yarn NodeManager as a auxiliary service in NM.
 */
public class YarnShuffleService extends AuxiliaryService {

	private static final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

	/**
	 * Define the name of flink shuffle service, it has the following usages:
	 * (1) Configure shuffle service in NodeManger in yarn-site.xml
	 * (2) Suggest the auxiliary service name of shuffle service in NodeManger
	 * (3) Yarn(Session)ResourceManager need to configure its serviceData in
	 * 		ContainerLaunchContext so that flink shuffle service will get method
	 * 		initializeApplication() being invoked. Furthermore we can add more
	 * 		information through service data if we want to add authentication
	 * 		mechanism.
	 */
	public static final String YARN_SHUFFLE_SERVICE_NAME = "yarn_shuffle_service_for_flink";

	/**
	 * Configuration key to suggest whether to stop NodeManager if fails to
	 * initialize YarnShuffleService. Set true by default.
	 */
	public static final String STOP_ON_FAILURE = "flink.shuffle-service.stop-on-failure";

	private ExternalBlockShuffleService shuffleServer = null;

	public YarnShuffleService() {
		super(YARN_SHUFFLE_SERVICE_NAME);
		logger.info("Initializing YARN shuffle service for flink");
	}

	/**
	 * Starts the shuffle server with the given configuration.
	 */
	@Override
	protected void serviceInit(Configuration incomingConf) throws Exception {

		boolean stopOnFailure = incomingConf.getBoolean(STOP_ON_FAILURE, true);

		try {
			shuffleServer = new ExternalBlockShuffleService(fromHadoopConfiguration(incomingConf));
			shuffleServer.start();
		} catch (Exception e) {
			logger.error("Fails to start YARN shuffle service for flink");
			if (stopOnFailure) {
				throw e;
			} else {
				noteFailure(e);
			}
		}
	}

	@Override
	public void initializeApplication(ApplicationInitializationContext context) {
		String user = context.getUser();
		String appId = context.getApplicationId().toString();
		shuffleServer.initializeApplication(user, appId);
		logger.info("Initialize Application, user: {}, appId: {}", user, appId);
	}

	@Override
	public void stopApplication(ApplicationTerminationContext context) {
		String appId = context.getApplicationId().toString();
		shuffleServer.stopApplication(appId);
		logger.info("Stop Application for {}.", appId);
	}

	@Override
	public void initializeContainer(ContainerInitializationContext context) {
		ContainerId containerId = context.getContainerId();
		logger.info("Initialize container {}", containerId);
	}

	@Override
	public void stopContainer(ContainerTerminationContext context) {
		ContainerId containerId = context.getContainerId();
		logger.info("Stop container {}", containerId);
	}

	/**
	 * Closes the shuffle server to clean up any associated state.
	 */
	@Override
	protected void serviceStop() {
		if (shuffleServer != null) {
			shuffleServer.stop();
		}
		logger.info("Stop YARN shuffle service for flink");
	}

	/** Currently this method is of no use. */
	@Override
	public ByteBuffer getMetaData() {
		return ByteBuffer.allocate(0);
	}

	// --------------------------- Utilities -------------------------------

	/**
	 * Generate flink configuration from hadoop configuration.
	 * @param hadoopConf The hadoop configuration.
	 * @return The corresponding link configuration.
	 */
	private static org.apache.flink.configuration.Configuration fromHadoopConfiguration(Configuration hadoopConf) {
		org.apache.flink.configuration.Configuration flinkConf = new org.apache.flink.configuration.Configuration();

		// Copy all the original configurations to flinkConf.
		Iterator<Map.Entry<String, String>> iterator = hadoopConf.iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			flinkConf.setString(entry.getKey(), entry.getValue());
		}

		// Use nm-local-dirs if flink shuffle service's configuration is not set.
		String nmLocalDirs = hadoopConf.get(YarnConfiguration.NM_LOCAL_DIRS, "");
		String flinkLocalDirs = hadoopConf.get(ExternalBlockShuffleServiceOptions.LOCAL_DIRS.key(), "");
		if (!nmLocalDirs.isEmpty() && flinkLocalDirs.isEmpty()) {
			flinkConf.setString(ExternalBlockShuffleServiceOptions.LOCAL_DIRS.key(), nmLocalDirs);
		}

		return flinkConf;
	}
}
