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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.ResourceSpecInfo;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * Request body for a savepoint disposal call.
 */
public class UpdatingJobRequest implements RequestBody {

	private static final String field_name_vertex_parallelism_resource = "vertex-parallelism-resource";

	@JsonProperty(field_name_vertex_parallelism_resource)
	private final Map<String, Tuple2<Integer, ResourceSpecInfo>> vertexParallelismResource;

	@JsonCreator
	public UpdatingJobRequest(
		@JsonProperty(field_name_vertex_parallelism_resource)
		@Nonnull Map<String, Tuple2<Integer, ResourceSpecInfo>> vertexParallelismResource) {
		this.vertexParallelismResource = vertexParallelismResource;
	}

	@JsonIgnore
	public Map<String, Tuple2<Integer, ResourceSpecInfo>> getVertexParallelismResource() {
		return vertexParallelismResource;
	}
}
