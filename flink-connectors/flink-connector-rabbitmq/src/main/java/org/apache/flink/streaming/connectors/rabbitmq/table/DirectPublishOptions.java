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

package org.apache.flink.streaming.connectors.rabbitmq.table;

import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.types.Row;

import com.rabbitmq.client.AMQP;

/**
 * DirectPublishOptions for publishing messages directly to a queue specified by a column.
 */
public class DirectPublishOptions implements RMQSinkPublishOptions<Row> {

	private int queueNameIndex;

	public DirectPublishOptions(int queueNameIndex) {
		this.queueNameIndex = queueNameIndex;
	}

	@Override
	public String computeRoutingKey(Row a) {
		return a.getField(queueNameIndex).toString();
	}

	@Override
	public AMQP.BasicProperties computeProperties(Row a) {
		return null;
	}

	@Override
	public String computeExchange(Row a) {
		return "";
	}
}
