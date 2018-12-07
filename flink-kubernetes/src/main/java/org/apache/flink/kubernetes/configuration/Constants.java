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

package org.apache.flink.kubernetes.configuration;

/**
 * Constants for kubernetes.
 */
public final class Constants {

	public static final String JOBMANAGER_RPC_PORT = "rpc";

	public static final String JOBMANAGER_BLOB_PORT = "blob";

	public static final String JOBMANAGER_REST_PORT = "rest";

	public static final String FLINK_CONF_VOLUME = "flink-conf-volume";

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback-kubernetes.xml";

	public static final String CONFIG_FILE_LOG4J_NAME = "log4j-kubernetes.properties";

	public static final String NAME_SEPARATOR = "-";

	public static final String JOBMANAGER_RC_NAME_SUFFIX = "-jobmanager-rc";

	public static final String JOB_MANAGER_NAME_SUFFIX = "-jobmanager";

	public static final String SERVICE_NAME_SUFFIX = "-service";

	public static final String JOB_MANAGER_CONFIG_MAP_SUFFIX =
		"-jobmanager-conf-map";

	public static final String TASK_MANAGER_CONFIG_MAP_SUFFIX =
		"-taskmanager-conf-map";

	public static final String TASK_MANAGER_LABEL_SUFFIX =
		"-taskmanager";

	public static final String TASK_MANAGER_RPC_PORT = "rpc";

	public static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	public static final String LABEL_APP_KEY = "app";

	public static final String LABEL_COMPONENT_KEY = "component";

	public static final String LABEL_COMPONENT_TASK_MANAGER = "taskmanager";

	public static final String LABEL_PRIORITY_KEY = "priority";
}
