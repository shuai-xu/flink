/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Utils class for loading Kafka Consumer & Producer.
 */
public class KafkaUtils {

	// load KafkaConsumer via system classloader instead of application classloader, otherwise we will hit classloading
	// issue in scala-shell scenario where kafka jar is not shipped with JobGraph, but shipped when starting yarn
	// container.
	public static KafkaConsumer createKafkaConsumer(Properties kafkaProperties) {
		ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(null);
			return new KafkaConsumer<>(kafkaProperties);
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	// load KafkaProducer via system classloader instead of application classloader, otherwise we will hit classloading
	// issue in scala-shell scenario where kafka jar is not shipped with JobGraph, but shipped when starting yarn
	// container.
	public static KafkaProducer createKafkaProducer(Properties kafkaProperties) {
		ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(null);
			return new KafkaProducer<>(kafkaProperties);
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
	}
}
