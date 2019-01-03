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

import org.apache.flink.runtime.healthmanager.HealthMonitor;

/**
 * the Symptom Detector.
 */
public interface Detector {

	/**
	 * Init the Detector, which may subscribe some metric from metric provider.
	 * @param monitor the HeathMonitor which loads the detector.
	 */
	void open(HealthMonitor monitor);

	/**
	 * Close the Detector.
	 */
	void close();

	/**
	 * Detect the symptom, return null if not abnormal symptom detected.
	 * @return
	 */
	Symptom detect();

}
