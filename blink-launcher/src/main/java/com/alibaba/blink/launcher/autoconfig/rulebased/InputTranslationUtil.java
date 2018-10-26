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

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.blink.launcher.util.CollectionUtil;
import com.alibaba.blink.launcher.util.NumUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Map;

/**
 * Translate input into what we need.
 */
public class InputTranslationUtil {
	private static final Logger LOG = LoggerFactory.getLogger(InputTranslationUtil.class);

	// Only take up to 6 decimals
	private static final DecimalFormat DOUBLE_FORMAT = new DecimalFormat("#.######");

	/**
	 * If targetSourceTps is specified with source table name, transform that into vertex id.
	 */
	private static Map<Integer, Double> transformTargetSourceTps(Map<String, Double> targetSourceTps, Map<Integer, String> sourceVerticesMap) {
		return targetSourceTps.entrySet().stream()
			.map(e -> {
				Integer key;
				if (NumUtil.isInt(e.getKey())) {
					key = Integer.parseInt(e.getKey());
				} else {
					key = getVertexIdBySourceName(e.getKey(), sourceVerticesMap);
				}

				return new Tuple2<>(key, e.getValue());
			})
			.collect(CollectionUtil.toLinkedHashMap(t -> t.f0, t -> t.f1));
	}

	/**
	 * Get the source vertex id by sourceName from vertexProfiles. Throws exception if unfound.
	 *
	 * @param tableName		source table name that user specified in Blink SQL
	 * @param sourceVerticesMap	[vertex_id, vertex_name]
	 * @return id of the source vertex that contains the table name
	 * @throws IllegalArgumentException if no source vertex has the table name
	 */
	public static Integer getVertexIdBySourceName(String tableName, Map<Integer, String> sourceVerticesMap) {
		// Matching pattern: "Source: <source_type>-tableName -> <following nodes> ...."
		String filter = String.format("-%s-Stream ", tableName);
		// Matching pattern: "Source: <source_type>-tableName"
		String filter2 = String.format("-%s-Stream", tableName);

		return sourceVerticesMap.entrySet().stream()
			.filter(e -> e.getValue().contains(filter) || e.getValue().endsWith(filter2))
			.findFirst()
			.map(e -> e.getKey())
			.orElseThrow(() -> new IllegalArgumentException(
				String.format("Cannot find corresponding vertex of source '%s' from sourceVerticesMap '%s'. Please enter a valid source name",
					tableName, sourceVerticesMap)
			));
	}

	/**
	 * Calculate a reasonable parallelism based on experience.
	 *
	 * @param sourcePartitionNum source partition num
	 * @return a reasonable parallelism
	 */
	public static int calSuitableParallelism(int sourcePartitionNum) {
		if (sourcePartitionNum <= 0) {
			return 1;
		} else if (sourcePartitionNum <= 4) {
			return sourcePartitionNum;
		} else if (sourcePartitionNum <= 16) {
			return (int) Math.ceil(sourcePartitionNum / 2.0);
		} else if (sourcePartitionNum <= 64) {
			return Math.max(8, (int) Math.ceil(sourcePartitionNum / 3.0));
		} else if (sourcePartitionNum <= 256) {
			return Math.max(32, (int) Math.ceil(sourcePartitionNum / 4.0));
		} else {
			return Math.min(512, (int) Math.ceil(sourcePartitionNum / 4.0));
		}
	}

	/**
	 * Calculate a reasonable initial parallelism factor.
	 *
	 * <p>Partitions       ParallelismFactor
	 * 0				1
	 * [1, 4]			[1, 4]
	 * [5, 16]			[2, 8]
	 * [17, 64]			[8, 16]
	 * [65, 128]		[16, 32]
	 * [128, ...		32
	 *
	 * @param sourcePartitionNum source partition num
	 * @return a reasonable initial parallelism factor
	 */
	public static int calReasonableInitialParallelismFactor(int sourcePartitionNum) {
		if (sourcePartitionNum <= 0) {
			return 1;
		} else if (sourcePartitionNum <= 4) {
			return sourcePartitionNum;
		} else if (sourcePartitionNum <= 16) {
			return (int) Math.ceil(sourcePartitionNum / 2.0);
		} else if (sourcePartitionNum <= 64) {
			return Math.max(8, (int) Math.ceil(sourcePartitionNum / 4.0));
		} else if (sourcePartitionNum <= 128) {
			return Math.max(16, (int) Math.ceil(sourcePartitionNum / 4.0));
		} else {
			return 32;
		}
	}
}
