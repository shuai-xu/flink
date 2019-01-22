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

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResourceSpecInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * Request body for a updating job.
 */
public class UpdatingJobRequest implements RequestBody {

	private static final String FIELD_NAME_VERTEX_PARALLELISM_RESOURCE = "vertex-parallelism-resource";

	@JsonProperty(FIELD_NAME_VERTEX_PARALLELISM_RESOURCE)
	private final Map<String, VertexResource> vertexParallelismResource;

	@JsonCreator
	public UpdatingJobRequest(
		@JsonProperty(FIELD_NAME_VERTEX_PARALLELISM_RESOURCE)
		@Nonnull Map<String, VertexResource> vertexParallelismResource) {
		this.vertexParallelismResource = vertexParallelismResource;
	}

	@JsonIgnore
	public Map<String, VertexResource> getVertexParallelismResource() {
		return vertexParallelismResource;
	}

	/**
	 * vertex resource for update job.
	 */
	public static final class VertexResource {
		private static final String FIELD_NAME_VERTEX_PARALLELISM = "parallelism";
		private static final String FIELD_NAME_VERTEX_RESOURCE = "resource";

		@JsonProperty(FIELD_NAME_VERTEX_PARALLELISM)
		private  final Integer parallelism;

		@JsonProperty(FIELD_NAME_VERTEX_RESOURCE)
		private  final ResourceSpecInfo resource;

		@JsonCreator
		public VertexResource(@JsonProperty(FIELD_NAME_VERTEX_PARALLELISM) Integer parallelism,
			@JsonProperty(FIELD_NAME_VERTEX_RESOURCE) ResourceSpecInfo resource) {
			this.parallelism = parallelism;
			this.resource = resource;
		}

		@JsonIgnore
		public Integer getParallelism() {
			return parallelism;
		}

		@JsonIgnore
		public ResourceSpecInfo getResource() {
			return resource;
		}
	}

}
