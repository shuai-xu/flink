/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionMode;

/**
 * Defines the scheduling mode between two operators of a stream edge.
 */
public enum StreamSchedulingMode {

	/**
	 * The consumer and producer will be start at the same time.
	 */
	CONCURRENT,

	/**
	 * The consumer will be started when the producer is consumable.
	 */
	SEQUENTIAL,

	/**
	 * Indicates to schedule based on {@link ExecutionMode} and later chaining. If {@link ExecutionMode}
	 * is BATCH and the stream edge is a outer edge of a chain, it is SEQUENTIAL. Otherwise, it is CONCURRENT.
	 */
	AUTO
}
