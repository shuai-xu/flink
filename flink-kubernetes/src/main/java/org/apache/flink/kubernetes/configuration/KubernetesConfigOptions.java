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

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's kubernetes runners.
 *
 * <p>These options are not expected to be ever configured by users explicitly.
 */
public class KubernetesConfigOptions {

	public static final ConfigOption<String> CLUSTER_ID =
		key("kubernetes.cluster-id")
		.noDefaultValue()
		.withDescription("The kubernetes cluster-id. It could be specified by -n or generate a random id.");

	public static final ConfigOption<String> CONF_DIR =
		key("kubernetes.flink.conf.dir")
			.defaultValue("/etc/flink/conf")
			.withDescription("The conf dir will be mounted in pod.");

	public static final ConfigOption<String> MASTER_URL =
		key("kubernetes.master.url")
		.defaultValue("localhost:8080")
		.withDescription("The kubernetes master url.");

	public static final ConfigOption<String> NAME_SPACE =
		key("kubernetes.namespace")
			.defaultValue("default")
			.withDescription("The namespace that will be used for running the jobmanager and taskmanager pods.");

	public static final ConfigOption<String> SERVICE_EXTERNAL_ADDRESS =
		key("kubernetes.service.external.address")
			.defaultValue("localhost:8081")
			.withDescription("The external address of kubernetes service to submit job and get webui/dashboard." +
				"It could be a local proxy started by kubectl " +
				"e.g. kubectl port-forward service/flink-default-session-service 8081. " +
				"Or it could be ClusterIP/NodePort/LoadBalance/ExternalName/Ingress exposed when starting kubernetes service.");

	public static final ConfigOption<Double> JOB_MANAGER_CORE =
		key("kubernetes.jobmanager.cpu")
		.defaultValue(1.0)
		.withDescription("The number of cpu used by job manager");

	public static final ConfigOption<String> CONTAINER_IMAGE =
		key("kubernetes.container.image")
		.defaultValue("flink-k8s:latest")
		.withDescription("Container image to use for Flink containers. Individual container types " +
			"(e.g. jobmanager or taskmanager) can also be configured to use different images if desired, " +
			"by setting the container type-specific image name.");

	public static final ConfigOption<String> CONTAINER_IMAGE_PULL_POLICY =
		key("kubernetes.container.image.pullPolicy")
		.defaultValue("IfNotPresent")
		.withDescription("Kubernetes image pull policy. Valid values are Always, Never, and IfNotPresent.");

	public static final ConfigOption<String> CONTAINER_START_COMMAND_TEMPLATE =
		key("kubernetes.container-start-command-template")
		.defaultValue("%java% %classpath% %jvmmem% %jvmopts% %logging% %class%")
		.withDescription("Template for the kubernetes container start invocation");

	public static final ConfigOption<String> JOBMANAGER_POD_NAME =
		key("kubernetes.jobmanager.pod.name")
		.defaultValue("jobmanager")
		.withDescription("Name of the jobmanager pod.");

	public static final ConfigOption<String> JOB_MANAGER_CONTAINER_NAME =
		key("kubernetes.jobmanager.container.name")
		.defaultValue("flink-kubernetes-jobmanager")
		.withDescription("Name of the jobmanager container.");

	public static final ConfigOption<String> JOB_MANAGER_CONTAINER_IMAGE =
		key("kubernetes.jobmanager.container.image")
		.noDefaultValue()
		.withDescription("Container image to use for the jobmanager.");

	public static final ConfigOption<Integer> TASK_MANAGER_COUNT =
		key("kubernetes.taskmanager.count")
		.defaultValue(1)
		.withDescription("The task manager count for session cluster.");

	public static final ConfigOption<Long> TASK_MANAGER_REGISTER_TIMEOUT =
		key("kubernetes.taskmanager.register-timeout")
			.defaultValue(120L)
			.withDescription("The register timeout for a task manager before released by resource manager. In seconds." +
				"In case of a task manager took very long time to be launched.");

	public static final ConfigOption<Integer> WORKER_NODE_MAX_FAILED_ATTEMPTS =
		key("kubernetes.workernode.max-failed-attempts")
			.defaultValue(100)
			.withDescription("The max failed attempts for work node.");
}
