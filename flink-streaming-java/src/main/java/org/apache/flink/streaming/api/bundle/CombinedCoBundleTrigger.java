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

package org.apache.flink.streaming.api.bundle;

import org.apache.flink.util.Preconditions;

/**
 * combined trigger for KeyedCoBundleOperator.
 * @param <L>
 * @param <R>
 */
public class CombinedCoBundleTrigger<L, R> implements CoBundleTrigger<L, R> {

	private final CoBundleTrigger<L, R>[] triggers;

	public CombinedCoBundleTrigger(CoBundleTrigger<L, R>[] triggers) {
		Preconditions.checkArgument(triggers.length > 0, "number of triggers must be greater than 0");
		this.triggers = triggers;
	}

	@Override
	public void registerBundleTriggerCallback(BundleTriggerCallback callback, BundleTriggerContext context) {
		for (CoBundleTrigger trigger : triggers) {
			trigger.registerBundleTriggerCallback(callback, context);
		}
	}

	@Override
	public void onLeftElement(L element) throws Exception {
		for (CoBundleTrigger trigger : triggers) {
			trigger.onLeftElement(element);
		}
	}

	@Override
	public void onRightElement(R element) throws Exception {
		for (CoBundleTrigger trigger : triggers) {
			trigger.onRightElement(element);
		}
	}

	@Override
	public void reset() {
		for (CoBundleTrigger trigger : triggers) {
			trigger.reset();
		}
	}

	@Override
	public String explain() {
		StringBuilder sb = new StringBuilder("CombinedCoBundleTrigger: ");
		for (int i = 0; i < triggers.length; i++) {
			if (i > 0) {
				sb.append(" ; ");
			}
			sb.append(triggers[i].explain());
		}
		return sb.toString();
	}
}
