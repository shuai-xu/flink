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

package org.apache.flink.table.client.config;

import org.apache.flink.annotation.VisibleForTesting;

import java.io.IOException;
import java.net.URL;
import java.util.Map;


/**
 * Environment configuration that represents the content of an environment file. Environment files
 * define tables, execution, and deployment behavior. An environment might be defined by default or
 * as part of a session. Environments can be merged or enriched with properties (e.g. from CLI command).
 *
 * <p>In future versions, we might restrict the merging or enrichment of deployment properties to not
 * allow overwriting of a deployment by a session.
 */
public class Environment {

	private Execution execution;

	private Deployment deployment;

	public Environment() {
		this.execution = new Execution();
		this.deployment = new Deployment();
	}

	public void setExecution(Map<String, Object> config) {
		this.execution = Execution.create(config);
	}

	public Execution getExecution() {
		return execution;
	}

	public void setDeployment(Map<String, Object> config) {
		this.deployment = Deployment.create(config);
	}

	public Deployment getDeployment() {
		return deployment;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Parses an environment file from an URL.
	 */
	public static Environment parse(URL url) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(url, Environment.class);
	}

	/**
	 * Parses an environment file from an String.
	 */
	@VisibleForTesting
	public static Environment parse(String content) throws IOException {
		return new ConfigUtil.LowerCaseYamlMapper().readValue(content, Environment.class);
	}

	/**
	 * Merges two environments. The properties of the first environment might be overwritten by the second one.
	 */
	public static Environment merge(Environment env1, Environment env2) {
		final Environment mergedEnv = new Environment();

		// merge execution properties
		mergedEnv.execution = Execution.merge(env1.getExecution(), env2.getExecution());

		// merge deployment properties
		mergedEnv.deployment = Deployment.merge(env1.getDeployment(), env2.getDeployment());

		return mergedEnv;
	}

	/**
	 * Enriches an environment with new/modified properties and returns the new instance.
	 */
	public static Environment enrich(Environment env, Map<String, String> properties) {
		final Environment enrichedEnv = new Environment();

		// enrich execution properties
		enrichedEnv.execution = Execution.enrich(env.execution, properties);

		// enrich deployment properties
		enrichedEnv.deployment = Deployment.enrich(env.deployment, properties);

		return enrichedEnv;
	}

}
