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

package org.apache.flink.table.catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a function in catalog.
 */
public class CatalogFunction {
	private Source source; // Source of the function
	private String clazzName; // Fully qualified class name of the function
	private Map<String, String> properties;

	public CatalogFunction(String clazz) {
		this(clazz, new HashMap<>());
	}

	public CatalogFunction(String clazz, Map<String, String> properties) {
		this(Source.CLASS, clazz, properties);
	}

	public CatalogFunction(Source source, String clazzName, Map<String, String> properties) {
		this.source = source;
		this.clazzName = clazzName;
		this.properties = properties;
	}

	public Source getSource() {
		return source;
	}

	public String getClazzName() {
		return clazzName;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	/**
	 * Source of a function.
	 */
	public enum Source {
		CLASS
	}

	public CatalogFunction deepCopy() {
		return new CatalogFunction(source, clazzName, new HashMap(properties));
	}

	@Override
	public String toString() {
		return "CatalogFunction{" +
			"source=" + source +
			", clazzName='" + clazzName + '\'' +
			", properties=" + properties +
			'}';
	}
}
