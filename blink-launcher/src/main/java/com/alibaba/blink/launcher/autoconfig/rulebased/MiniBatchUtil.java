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

import org.apache.flink.streaming.api.bundle.BundleTrigger;
import org.apache.flink.streaming.api.bundle.CombinedBundleTrigger;
import org.apache.flink.streaming.api.bundle.CountBundleTrigger;
import org.apache.flink.streaming.api.bundle.TimeBundleTrigger;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.operator.bundle.BundleOperator;
import org.apache.flink.table.runtime.operator.bundle.KeyedBundleOperator;

import com.alibaba.blink.launcher.autoconfig.StreamNodeProperty;

import java.lang.reflect.Field;

/**
 * Utils to setup mini batch configuration.
 */
public class MiniBatchUtil {

	/**
	 * Extract mini batch property from stream node to property.
	 * @param property   the property of stream node
	 * @param streamNode the stream node.
	 */
	public static void trySetMiniBatchProperties(StreamNodeProperty property, StreamNode streamNode) {
		StreamOperator operator = streamNode.getOperator();
		if (operator instanceof KeyedBundleOperator || operator instanceof BundleOperator) {
			try {
				Field triggerField = operator.getClass().getDeclaredField("bundleTrigger");
				triggerField.setAccessible(true);
				BundleTrigger bundleTrigger = (BundleTrigger) triggerField.get(operator);
				if (bundleTrigger instanceof CombinedBundleTrigger) {
					CombinedBundleTrigger combinedBundleTrigger = (CombinedBundleTrigger)
						bundleTrigger;
					Field triggersField = CombinedBundleTrigger.class.getDeclaredField("triggers");
					triggersField.setAccessible(true);
					BundleTrigger[] triggers = (BundleTrigger[]) triggersField.get(combinedBundleTrigger);
					for (BundleTrigger trigger : triggers) {
						if (trigger instanceof TimeBundleTrigger) {
							TimeBundleTrigger timeBundleTrigger = (TimeBundleTrigger) trigger;
							Field timeoutField = TimeBundleTrigger.class.getDeclaredField("timeout");
							timeoutField.setAccessible(true);
							long timeout = (Long) timeoutField.get(timeBundleTrigger);
							property.setMiniBatchAllowLatency(timeout);
						} else if (trigger instanceof CountBundleTrigger) {
							CountBundleTrigger countBundleTrigger = (CountBundleTrigger) trigger;
							Field countField = CountBundleTrigger.class.getDeclaredField("maxCount");
							countField.setAccessible(true);
							long count = (Long) countField.get(countBundleTrigger);
							property.setMiniBatchSize(count);
						}
					}
				}
			} catch (Exception e) {
				// do nothing
			}
		}
	}

	/**
	 * Apply mini batch property from stream node property to stream node.
	 * @param property the property to apply
	 * @param node     the stream node set.
	 */
	public static void tryApplyMiniBatchProperties(StreamNodeProperty property, StreamNode node) {

		if (property.getMiniBatchSize() == 0 && property.getMiniBatchAllowLatency() == 0) {
			return;
		}

		StreamOperator operator = node.getOperator();

		if (operator instanceof KeyedBundleOperator || operator instanceof BundleOperator) {

			CombinedBundleTrigger<BaseRow> combinedBundleTrigger;
			if (property.getMiniBatchSize() > 0 && property.getMiniBatchAllowLatency() > 0) {
				combinedBundleTrigger = new CombinedBundleTrigger<>(
					new TimeBundleTrigger<>(property.getMiniBatchAllowLatency()),
					new CountBundleTrigger<>(property.getMiniBatchSize()));
			} else if (property.getMiniBatchSize() > 0) {
				combinedBundleTrigger = new CombinedBundleTrigger<>(
					new CountBundleTrigger<>(property.getMiniBatchSize()));
			} else if (property.getMiniBatchAllowLatency() > 0) {
				combinedBundleTrigger = new CombinedBundleTrigger<>(
					new TimeBundleTrigger<>(property.getMiniBatchAllowLatency()));
			} else {
				combinedBundleTrigger = new CombinedBundleTrigger<>(
					new CountBundleTrigger<>(1));
			}
			try {
				Field triggerField = operator.getClass().getDeclaredField("bundleTrigger");
				triggerField.setAccessible(true);
				triggerField.set(operator, combinedBundleTrigger);
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}
}
