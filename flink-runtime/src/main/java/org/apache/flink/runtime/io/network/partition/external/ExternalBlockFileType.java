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

package org.apache.flink.runtime.io.network.partition.external;

public enum ExternalBlockFileType {
	/**
	 *  Undefined external file type
	 */
	UNDEFINED_SUBPARTITION_FILE,

	/**
	 *  External file which has only one subpartition in each file
	 */
	SINGLE_SUBPARTITION_FILE,

	/**
	 *  External file(s) each of which has all subpartitions in one file
	 *  Notice that there might be multiple files and should concatenate data
	 *  of a subpartition from each file to get the overall subpartition data
	 *  in a partition.
	 */
	MULTI_SUBPARTITION_FILE
}
