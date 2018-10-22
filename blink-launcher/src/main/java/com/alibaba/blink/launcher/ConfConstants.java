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

package com.alibaba.blink.launcher;

/**
 * SqlRunner config constants.
 */
public class ConfConstants {

	// state backend type
	public static final String STATE_BACKEND_TYPE = "state.backend.type"; // gemini, rocksdb, niagara

	// niagara
	public static final String STATE_BACKEND_NIAGARA_TTL_MS = "state.backend.niagara.ttl.ms"; // 3 * 86400 * 1000

	// rocksDB
	public static final String STATE_BACKEND_ROCKSDB_TTL_MS = "state.backend.rocksdb.ttl.ms"; // 3 * 86400 * 1000

	// max parallelism
	public static final String STREAM_ENV_MAX_PARALLELISM = "stream.env.maxParallelism";

	// checkpoint interval
	public static final String CHECKPOINT_INTERVAL_MS = "blink.checkpoint.interval.ms";

	// checkpoint mode (EXACTLY_ONCE, AT_LEAST_ONCE)
	public static final String CHECKPOINT_MODE = "blink.checkpoint.mode";

	// checkpoint timeout
	public static final String CHECKPOINT_TIMEOUT_MS = "blink.checkpoint.timeout.ms";

	// checkpoint maximum number
	public static final String CHECKPOINT_MAX_CONCURRENT = "blink.checkpoint.max.concurrent";

	// checkpoint min pause
	public static final String CHECKPOINT_MIN_PAUSE_MS = "blink.checkpoint.min.pause.ms";

	// checkpoint fail on error (2.0)
	public static final String CHECKPOINT_FAIL_ON_ERROR = "blink.checkpoint.fail_on_checkpoint_error";
	public static final String DEFAULT_CHECKPOINT_FAIL_ON_ERROR = "false";

	// TimeCharacteristic (suport ProcessingTime, IngestionTime or EventTime)
	public static final String RECORD_TIMESTAMP_TYPE = "record.timestamp.type";

	// watermark interval
	public static final String AUTO_WATERMARK_INTERVAL_MS = "blink.auto.watermark.interval.ms";

	// operator name max length
	public static final String BLINK_OPERATOR_NAME_MAX_LENGTH = "blink.operator.name.max.length";

	// operator name default max length
	// mainly concerned with source name, which is in format of <source_type>-<table_name>
	// 128 allocated for table name, 39 for source name, and 1 for '-'
	public static final int DEFAULT_OPERATOR_NAME_MAX_LENGTH = 168;

	// enable object reuse
	public static final String OBJECT_REUSE = "blink.object.reuse";

	// enable codegen debug
	public static final String BLINK_CODEGEN_DEBUG = "blink.codegen.debug";

	public static final String BLINK_JOB_TIMEZONE = "blink.job.timeZone";

	//join reorder
	public static final String BLINK_JOINREORDER_ENABLED = "blink.joinreorder.enabled";

	public static final String BLINK_CODEGEN_REWRITE = "blink.codegen.rewrite";

}
