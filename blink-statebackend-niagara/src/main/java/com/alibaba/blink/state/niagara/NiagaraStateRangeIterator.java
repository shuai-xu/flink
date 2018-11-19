/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.state.niagara;

import org.apache.flink.util.Preconditions;

/**
 * State range iterator for Niagara.
 *
 * @param <T> The type of the values that of iterator in Niagara.
 */
public abstract class NiagaraStateRangeIterator<T> extends AbstractNiagaraStateIterator<T> {

	private final byte[] startDBKey;

	private final byte[] endDBKey;

	NiagaraStateRangeIterator(
			NiagaraTabletInstance instance,
			byte[] startDBKey,
			byte[] endDBKey
	) {
		super(instance);
		Preconditions.checkArgument(startDBKey != null,
			"start key bytes cannot be null when creating NiagaraStateRangeIterator.");
		Preconditions.checkArgument(endDBKey != null,
			"end key bytes cannot be null when creating NiagaraStateRangeIterator.");
		Preconditions.checkArgument(NiagaraTabletInstance.compare(startDBKey, endDBKey) <= 0,
			" start key bytes must order before end key bytes when creating NiagaraStateRangeIterator.");

		this.startDBKey = startDBKey;
		this.endDBKey = endDBKey;
	}

	@Override
	byte[] getStartDBKey() {
		return startDBKey;
	}

	@Override
	byte[] getEndDBKey() {
		return endDBKey;
	}
}
