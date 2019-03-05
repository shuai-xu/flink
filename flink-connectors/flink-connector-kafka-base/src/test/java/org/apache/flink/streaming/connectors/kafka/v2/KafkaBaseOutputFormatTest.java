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

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for KafkaBaseOutputFormat.
 */
public class KafkaBaseOutputFormatTest {

	@Test
	public void testClose() throws IOException {
		final String clientId = "testKafkaBaseOutputFormat";
		Properties props = new Properties();
		props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1111");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		KafkaBaseOutputFormat kafkaBaseOutputFormat =
				new KafkaBaseOutputFormat(
						"TestTopic",
						new DefaultKafkaConverter(),
						props) {
			@Override
			protected void flush() {

			}

			@Override
			public RuntimeContext getRuntimeContext() {
				return new MockStreamingRuntimeContext(false, 1, 1);
			}
		};

		kafkaBaseOutputFormat.open(1, 1);
		assertEquals(1, findThreads(Arrays.asList(clientId, "kafka-producer-network-thread")));
		kafkaBaseOutputFormat.close();
		assertEquals(0, findThreads(Arrays.asList(clientId, "kafka-producer-network-thread")));
	}

	private static int findThreads(List<String> keywords) {
		int i = 0;
		for (Thread t : Thread.getAllStackTraces().keySet()) {
			boolean match = true;
			for (String keyword : keywords) {
				if (!t.getName().contains(keyword)) {
					match = false;
					break;
				}
			}
			if (match) {
				i++;
			}
		}
		return i;
	}
}
