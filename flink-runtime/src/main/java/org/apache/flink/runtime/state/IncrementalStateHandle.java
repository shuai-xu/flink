/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

/**
 * Interface of handles which presents they are generated in incremental checkpoints.
 */
public interface IncrementalStateHandle {

	/**
	 * Returns the full size of the state in bytes for incremental checkpoints. If the size is not known, this
	 * method should return {@code 0}.
	 *
	 * <p>The values produced by this method are only used for incremental checkpoints.
	 *
	 * <p>Note for implementors: This method should not perform any I/O operations
	 * while obtaining the state size (hence it does not declare throwing an {@code IOException}).
	 * Instead, the state size should be stored in the state object, or should be computable from
	 * the state stored in this object.
	 * The reason is that this method is called frequently by several parts of the checkpointing
	 * and issuing I/O requests from this method accumulates a heavy I/O load on the storage
	 * system at higher scale.
	 *
	 * @return Size of the full state in bytes for incremental checkpoints.
	 */
	long getFullStateSize();
}
