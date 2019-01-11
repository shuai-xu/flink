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

package org.apache.flink.runtime.healthmanager.plugins;

import org.apache.flink.runtime.healthmanager.RestServerClient;
import org.apache.flink.runtime.healthmanager.metrics.MetricProvider;

/**
 * Action to resolve symptoms.
 */
public interface Action {

	/**
	 * Execute the action.
	 * @param restServerClient
	 */
	void execute(RestServerClient restServerClient) throws Exception;

	/**
	 * Validate the result of the execution of action.
	 * @param provider
	 * @param restServerClient
	 * @return
	 */
	boolean validate(MetricProvider provider, RestServerClient restServerClient) throws Exception;

	/**
	 * Rollback action of current action.
	 *
	 * @return
	 */
	Action rollback();
}
