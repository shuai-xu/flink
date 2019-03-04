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

package org.apache.flink.table.temptable.rpc;

import org.apache.flink.table.temptable.TableService;

/**
 * Defined the message type and bytes of the message type for TableService RPC.
 */
public class TableServiceMessage {

	/**
	 * Indicates this is a {@link TableService} getPartitions request.
	 */
	public static final byte GET_PARTITIONS = 1;

	/**
	 * Indicates this is a {@link TableService} write request.
	 */
	public static final byte WRITE = 2;

	/**
	 * Indicates this is a {@link TableService} delete partition request.
	 */
	public static final byte DELETE_PARTITION = 3;

	/**
	 * Indicates this is a {@link TableService} initialize partition request.
	 */
	public static final byte INITIALIZE_PARTITION = 4;

	/**
	 * Indicates this is a {@link TableService} register table partition request.
	 */
	public static final byte REGISTER_PARTITION = 5;

	/**
	 * Indicates this is a {@link TableService} unregister partitions request.
	 */
	public static final byte UNREGISTER_PARTITIONS = 6;

	/**
	 * Indicates this is a {@link TableService} has finished write table partition.
	 */
	public static final byte FINISH_PARTITION = 7;

	/**
	 * The bytes of GET_PARTITIONS.
	 */
	public static final byte[] GET_PARTITIONS_BYTES = new byte[] {1};

	/**
	 * The bytes of WRITE.
	 */
	public static final byte[] WRITE_BYTES = new byte[] {2};

	/**
	 * The bytes of DELETE_PARTITION.
	 */
	public static final byte[] DELETE_PARTITION_BYTES = new byte[] {3};

	/**
	 * The bytes of INITIALIZE_PARTITION.
	 */
	public static final byte[] INITIALIZE_PARTITION_BYTES = new byte[] {4};

	/**
	 * The bytes of REGISTER_PARTITION.
	 */
	public static final byte[] REGISTER_PARTITION_BYTES = new byte[] {5};

	/**
	 * The bytes of UNREGISTER_PARTITION.
	 */
	public static final byte[] UNREGISTER_PARTITIONS_BYTES = new byte[] {6};

	/**
	 * The bytes of REGISTER_PARTITION.
	 */
	public static final byte[] FINISH_PARTITION_BYTES = new byte[] {7};

	/**
	 * Indicates this is a successful request.
	 */
	public static final byte SUCCESS = 0;

	/**
	 * Indicates this is a failed request.
	 */
	public static final byte FAILURE = 1;

	/**
	 * The number of bytes of SUCCESS / FAILURE.
	 */
	public static final int RESPONSE_STATUS_LENGTH = 1;

	/**
	 * The bytes of SUCCESS.
	 */
	public static final byte[] SUCCESS_BYTES = new byte[] { 0 };

	/**
	 * The bytes of FAILURE.
	 */
	public static final byte[] FAILURE_BYTES = new byte[] { 1 };

}
