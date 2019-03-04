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

package org.apache.flink.table.runtime.window.aligned;

import org.apache.flink.table.api.window.TimeWindow;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/**
 * Tests for InternalAlignedWindowTriggers.
 */
public class InternalAlignedWindowTriggersTest {

	@Test
	public void testTumblingTrigger() {
		AlignedWindowTrigger trigger = InternalAlignedWindowTriggers.tumbling(Duration.ofSeconds(5), Duration.ofMillis(0));
		assertEquals(4999, trigger.nextTriggerTime(2323));
		assertEquals(9999, trigger.nextTriggerTime(4999));
		assertEquals(9999, trigger.nextTriggerTime(5000));
		assertEquals(9999, trigger.nextTriggerTime(6000));
		assertEquals(14999, trigger.nextTriggerTime(9999));
		assertEquals(14999, trigger.nextTriggerTime(10000));

		assertEquals(TimeWindow.of(0, 5000), trigger.nextTriggerWindow(2323));
		assertEquals(TimeWindow.of(5000, 10000), trigger.nextTriggerWindow(4999));
		assertEquals(TimeWindow.of(5000, 10000), trigger.nextTriggerWindow(5000));
		assertEquals(TimeWindow.of(5000, 10000), trigger.nextTriggerWindow(6000));
		assertEquals(TimeWindow.of(10000, 15000), trigger.nextTriggerWindow(9999));
		assertEquals(TimeWindow.of(10000, 15000), trigger.nextTriggerWindow(10000));
	}

	@Test
	public void testSlidingTrigger() {
		AlignedWindowTrigger trigger = InternalAlignedWindowTriggers.sliding(
			Duration.ofMillis(6), Duration.ofMillis(2), Duration.ofMillis(0));
		assertEquals(1, trigger.nextTriggerTime(0));
		assertEquals(3, trigger.nextTriggerTime(1));
		assertEquals(3, trigger.nextTriggerTime(2));
		assertEquals(5, trigger.nextTriggerTime(3));
		assertEquals(5, trigger.nextTriggerTime(4));
		assertEquals(7, trigger.nextTriggerTime(5));

		assertEquals(TimeWindow.of(-4, 2), trigger.nextTriggerWindow(0));
		assertEquals(TimeWindow.of(-2, 4), trigger.nextTriggerWindow(1));
		assertEquals(TimeWindow.of(-2, 4), trigger.nextTriggerWindow(2));
		assertEquals(TimeWindow.of(0, 6), trigger.nextTriggerWindow(3));
		assertEquals(TimeWindow.of(0, 6), trigger.nextTriggerWindow(4));
		assertEquals(TimeWindow.of(2, 8), trigger.nextTriggerWindow(5));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidSlidingTrigger() {
		InternalAlignedWindowTriggers.sliding(
			Duration.ofMillis(5), Duration.ofMillis(2), Duration.ofMillis(0));
	}

}
