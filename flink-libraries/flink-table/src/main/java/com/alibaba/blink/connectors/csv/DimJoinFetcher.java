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

package com.alibaba.blink.connectors.csv;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;

import com.alibaba.blink.cache.Cache;
import com.codahale.metrics.SlidingWindowReservoir;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A Common abstract class for DimJoin.
 */
public abstract class DimJoinFetcher extends AbstractRichFunction {

	private static final long serialVersionUID = -3591005897957294244L;

	protected DropwizardHistogramWrapper fetchLatency = null;
	protected DropwizardMeterWrapper fetchQPS = null;
	protected DropwizardMeterWrapper fetchHitQPS = null;
	protected DropwizardMeterWrapper cacheHitQPS = null;
	protected AtomicLong emptyKeyCounter = new AtomicLong(0);

	public void metricInit(RuntimeContext runtimeContext, Cache<?, ?> cache) {
		fetchLatency = runtimeContext.getMetricGroup().histogram("dimJoin.fetchLatency",
			new DropwizardHistogramWrapper(
				new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100))));

		fetchQPS = runtimeContext.getMetricGroup().meter("dimJoin.fetchQPS",
			new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

		fetchHitQPS = runtimeContext.getMetricGroup().meter("dimJoin.fetchHitQPS",
			new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

		cacheHitQPS = runtimeContext.getMetricGroup().meter("dimJoin.cacheHitQPS",
			new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

		runtimeContext.getMetricGroup().gauge("dimJoin.fetchHit", new Gauge<Double>() {
			@Override
			public Double getValue() {
				double fetchQPSRate = fetchQPS.getRate();
				if (fetchQPSRate < 0.01) {
					return 0.0;
				} else {
					return (fetchHitQPS.getRate() + cacheHitQPS.getRate()) / fetchQPSRate;
				}
			}
		});

		runtimeContext.getMetricGroup().gauge("dimJoin.cacheHit", new Gauge<Double>() {
			@Override
			public Double getValue() {
				double fetchQPSRate = fetchQPS.getRate();
				if (fetchQPSRate < 0.01) {
					return 0.0;
				} else {
					return cacheHitQPS.getRate() / fetchQPSRate;
				}
			}
		});

		getRuntimeContext().getMetricGroup().gauge("dimJoin.cacheSize", (Gauge<Long>) cache::size);

		runtimeContext.getMetricGroup().gauge("dimJoin.nullKeyCount", (Gauge<Long>) () -> emptyKeyCounter.get());
	}
}
