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

package org.apache.flink.yarn.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_CORE;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class YarnConfigOptions {

	/**
	 * The hostname or address where the application master RPC system is listening.
	 */
	public static final ConfigOption<String> APP_MASTER_RPC_ADDRESS =
			key("yarn.appmaster.rpc.address")
			.noDefaultValue()
			.withDescription("The hostname or address where the application master RPC system is listening.");

	/**
	 * The port where the application master RPC system is listening.
	 */
	public static final ConfigOption<Integer> APP_MASTER_RPC_PORT =
			key("yarn.appmaster.rpc.port")
			.defaultValue(-1)
			.withDescription("The port where the application master RPC system is listening.");

	/**
	 * Defines whether user-jars are included in the system class path for per-job-clusters as well as their positioning
	 * in the path. They can be positioned at the beginning ("FIRST"), at the end ("LAST"), or be positioned based on
	 * their name ("ORDER").
	 */
	public static final ConfigOption<String> CLASSPATH_INCLUDE_USER_JAR =
		key("yarn.per-job-cluster.include-user-jar")
			.defaultValue("ORDER")
			.withDescription("Defines whether user-jars are included in the system class path for per-job-clusters as" +
				" well as their positioning in the path. They can be positioned at the beginning (\"FIRST\"), at the" +
				" end (\"LAST\"), or be positioned based on their name (\"ORDER\"). Setting this parameter to" +
				" \"DISABLED\" causes the jar to be included in the user class path instead.");

	/**
	 * The vcores exposed by YARN.
	 */
	@Deprecated
	public static final ConfigOption<Integer> VCORES =
		key("yarn.containers.vcores")
		.defaultValue(-1)
		.withDescription("The number of virtual cores (vcores) per YARN container. The config option has been deprecated." +
			" Please use " + TASK_MANAGER_CORE.key() + " instead.");

	/**
	 * The maximum number of failed YARN containers before entirely stopping
	 * the YARN session / job on YARN.
	 * By default, we take the number of initially requested containers.
	 *
	 * <p>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> MAX_FAILED_CONTAINERS =
		key("yarn.maximum-failed-containers")
		.noDefaultValue()
		.withDescription("Maximum number of containers the system is going to reallocate in case of a failure.");

	/**
	 * Set the number of retries for failed YARN ApplicationMasters/JobManagers in high
	 * availability mode. This value is usually limited by YARN.
	 * By default, it's 1 in the standalone case and 2 in the high availability case.
	 *
	 * <p>>Note: This option returns a String since Integer options must have a static default value.
	 */
	public static final ConfigOption<String> APPLICATION_ATTEMPTS =
		key("yarn.application-attempts")
		.noDefaultValue()
		.withDescription("Number of ApplicationMaster restarts. Note that that the entire Flink cluster will restart" +
			" and the YARN Client will lose the connection. Also, the JobManager address will change and you’ll need" +
			" to set the JM host:port manually. It is recommended to leave this option at 1.");

	/**
	 * The heartbeat interval between the Application Master and the YARN Resource Manager.
	 */
	public static final ConfigOption<Integer> HEARTBEAT_DELAY_SECONDS =
		key("yarn.heartbeat-delay")
		.defaultValue(5)
		.withDescription("Time between heartbeats with the ResourceManager in seconds.");

	/**
	 * When a Flink job is submitted to YARN, the JobManager's host and the number of available
	 * processing slots is written into a properties file, so that the Flink client is able
	 * to pick those details up.
	 * This configuration parameter allows changing the default location of that file (for example
	 * for environments sharing a Flink installation between users)
	 */
	public static final ConfigOption<String> PROPERTIES_FILE_LOCATION =
		key("yarn.properties-file.location")
		.noDefaultValue()
		.withDescription("When a Flink job is submitted to YARN, the JobManager’s host and the number of available" +
			" processing slots is written into a properties file, so that the Flink client is able to pick those" +
			" details up. This configuration parameter allows changing the default location of that file" +
			" (for example for environments sharing a Flink installation between users).");

	/**
	 * The config parameter defining the Akka actor system port for the ApplicationMaster and
	 * JobManager.
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234".
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<String> APPLICATION_MASTER_PORT =
		key("yarn.application-master.port")
		.defaultValue("0")
		.withDescription("With this configuration option, users can specify a port, a range of ports or a list of ports" +
			" for the Application Master (and JobManager) RPC port. By default we recommend using the default value (0)" +
			" to let the operating system choose an appropriate port. In particular when multiple AMs are running on" +
			" the same physical host, fixed port assignments prevent the AM from starting. For example when running" +
			" Flink on YARN on an environment with a restrictive firewall, this option allows specifying a range of" +
			" allowed ports.");

	/**
	 * This configuration option defines Application Master (and JobManager) can only run
	 * on nodes with the specific node label.
	 * By default it will use default node label of YARN cluster.
	 */
	public static final ConfigOption<String> APPLICATION_MASTER_NODE_LABEL =
		key("yarn.application-master.node-label")
			.noDefaultValue()
			.withDescription("This configuration option defines Application Master (and JobManager) can only run" +
				" on nodes with the specific node label. By default it will use default node label of YARN cluster.");

	/**
	 * A comma-separated list of strings to use as YARN application tags.
	 */
	public static final ConfigOption<String> APPLICATION_TAGS =
		key("yarn.tags")
		.defaultValue("")
		.withDescription("A comma-separated list of tags to apply to the Flink YARN application.");

	/**
	 * How many virtual core will use a physical core in yarn.
	 */
	public static final ConfigOption<Integer> YARN_VCORE_RATIO =
			key("yarn.vcore-ratio")
					.defaultValue(1)
					.withDeprecatedKeys("yarn.vcore.ratio");

	/**
	 * Physical core for Job JobManager (App Master).
	 */
	public static final ConfigOption<Integer> JOB_APP_MASTER_CORE =
			key("job.app-master-core")
					.defaultValue(1);

	/**
	 * The number of threads to start yarn containers in yarn resource manager.
	 */
	public static final ConfigOption<Integer> CONTAINER_LAUNCHER_NUMBER =
			key("yarn.container-launcher-number")
					.defaultValue(100);

	/**
	 * The register timeout for a container before released by resource manager. In seconds.
	 * In case of a container took very long time to be launched.
	 */
	public static final ConfigOption<Long> CONTAINER_REGISTER_TIMEOUT =
			key("yarn.container-register-timeout")
					.defaultValue(120L);

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated. */
	private YarnConfigOptions() {}

	/** @see YarnConfigOptions#CLASSPATH_INCLUDE_USER_JAR */
	public enum UserJarInclusion {
		DISABLED,
		FIRST,
		LAST,
		ORDER
	}
}
