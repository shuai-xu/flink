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

package org.apache.flink.streaming.examples.autoscale;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.metrics.SumAndCount;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * ParallelSourceTests.
 */
public class ParallelSourceExample {

	public static void main(String[] args) throws Exception {
		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setTracingMetricsEnabled(true);
		env.getConfig().setTracingMetricsInterval(1);
		env.addSource(new MyParallelSource()).setParallelism(5).map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		}).setParallelism(5).map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				Thread.sleep(10);
				return value;
			}
		}).setParallelism(100).setMaxParallelism(128)
				.addSink(new DiscardingSink<>()).setParallelism(100);
		env.execute();
	}

	/**
	 * Test parallel source.
	 */
	public static class MyParallelSource extends AbstractRichFunction implements ParallelSourceFunction<String> {

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			SumAndCount partitionLatency = new SumAndCount("partitionLatency", getRuntimeContext().getMetricGroup());
			int partitionCount = 10;
			getRuntimeContext().getMetricGroup().gauge("partitionCount", new Gauge<Integer>() {
				@Override
				public Integer getValue() {
					if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
						return partitionCount;
					} else {
						return 0;
					}
				}
			});
			BlockingQueue<String> queue = new ArrayBlockingQueue<>(100);
			for (int i = getRuntimeContext().getIndexOfThisSubtask(); i < partitionCount; i += getRuntimeContext().getNumberOfParallelSubtasks()) {
				new Thread(new Runnable() {
					@Override
					public void run() {
						ScaleUpExample.MyIterator iterator = new ScaleUpExample.MyIterator();
						while (true) {
							try {
								long startTime = System.nanoTime();
								String value = iterator.next();
								partitionLatency.update(System.nanoTime() - startTime);
								queue.put(value);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}).start();
			}
			while (true) {
				ctx.collect(queue.take());
			}

		}

		@Override
		public void cancel() {

		}
	}
}
