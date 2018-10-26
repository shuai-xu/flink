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

package com.alibaba.blink.launcher.autoconfig.rulebased;

import com.alibaba.blink.launcher.autoconfig.AbstractJsonSerializable;
import com.alibaba.blink.launcher.util.NumUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Properties;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ResourceSettings extends AbstractJsonSerializable {

	public static final String BLINK_NON_STATE_NATIVE_MEM_MB = "blink.non.state.native.mem.mb";

	public static final float DEFAULT_STATE_MEM_INCREASE_RATIO = 1.2f;

	private static final String DEFAULT_OPERATOR_CPU_CORES = "blink.resource.allocation.operator.default.cpu.cores";
	private static final double DEFAULT_DEFAULT_OPERATOR_CPU_CORES = 0.25;

	private static final String DEFAULT_OPERATOR_HEAP_MEMORY_MB = "blink.resource.allocation.operator.default.heap.mb";
	private static final int DEFAULT_DEFAULT_OPERATOR_HEAP_MEMORY_MB = 256;

	// operator name max length
	public static final String BLINK_OPERATOR_PARALLELISM = "blink.operator.parallelism";
	public static final int DEFAULT_OPERATOR_PARALLELISM = 1;

	// operator name max length
	public static final String BLINK_OPERATOR_NAME_MAX_LENGTH = "blink.operator.name.max.length";

	// operator name default max length
	// mainly concerned with source name, which is in format of <source_type>-<table_name>
	// 128 allocated for table name, 39 for source name, and 1 for '-'
	public static final int DEFAULT_OPERATOR_NAME_MAX_LENGTH = 168;

	private static final String PLAN_CHANGED_MESSAGE = "blink.autoconf.plan.changed.message";
	private static final String DEFAULT_PLAN_CHANGED_MESSAGE = "当前任务plan有变化，使用默认配置。";

	private Properties properties;

	public ResourceSettings(Properties properties) {
		this.properties = properties;
	}

	public double getDefaultOperatorCpuCores() {
		return NumUtil.getProperty(properties, DEFAULT_OPERATOR_CPU_CORES, DEFAULT_DEFAULT_OPERATOR_CPU_CORES);
	}

	public int getDefaultOperatorHeapMemoryMb() {
		return NumUtil.getProperty(properties, DEFAULT_OPERATOR_HEAP_MEMORY_MB, DEFAULT_DEFAULT_OPERATOR_HEAP_MEMORY_MB);
	}

	public String getPlanChangedMessage() {
		return properties.getProperty(PLAN_CHANGED_MESSAGE, DEFAULT_PLAN_CHANGED_MESSAGE);
	}

	public int getDefaultOperatorParallelism() {
		return NumUtil.parseInt(properties.getProperty(BLINK_OPERATOR_PARALLELISM), DEFAULT_OPERATOR_PARALLELISM);
	}

	public int getOperatorNameMaxLength() {
		return NumUtil.parseInt(properties.getProperty(BLINK_OPERATOR_NAME_MAX_LENGTH), DEFAULT_OPERATOR_NAME_MAX_LENGTH);
	}

}
