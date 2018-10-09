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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.TableProperties;

/**
 * RMQ connector options for table.
 */
public class RMQOptions {

	public static final ConfigOption<String> QUEUE_NAME_FIELD =
			ConfigOptions.key("queueField".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> MSG_FIELD =
			ConfigOptions.key("msgField".toLowerCase()).noDefaultValue();

	public static final ConfigOption<String> HOST =
			ConfigOptions.key("host".toLowerCase()).noDefaultValue();
	public static final ConfigOption<Integer> PORT =
			ConfigOptions.key("port".toLowerCase()).defaultValue(-1);
	public static final ConfigOption<String> VIRTUAL_HOST =
			ConfigOptions.key("virtualHost".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> USER_NAME =
			ConfigOptions.key("userName".toLowerCase()).noDefaultValue();
	public static final ConfigOption<String> PASSWORD =
			ConfigOptions.key("password".toLowerCase()).noDefaultValue();

	public static final ConfigOption<String> URI =
			ConfigOptions.key("uri".toLowerCase()).noDefaultValue();

	public static final ConfigOption<Integer> NETWORK_RECOVERY_INTERVAL =
			ConfigOptions.key("networkRecoveryInterval".toLowerCase()).defaultValue(-1);
	public static final ConfigOption<Boolean> AUTO_RECOVERY =
			ConfigOptions.key("autoRecovery".toLowerCase()).defaultValue(false);
	public static final ConfigOption<Boolean> TOPOLOGY_RECOVERY =
			ConfigOptions.key("topologyRecovery".toLowerCase()).defaultValue(false);

	public static final ConfigOption<Integer> CONNECTION_TIMEOUT =
			ConfigOptions.key("connectionTimeout".toLowerCase()).defaultValue(-1);
	public static final ConfigOption<Integer> REQUEST_CHANNEL_MAX =
			ConfigOptions.key("requestChannelMax".toLowerCase()).defaultValue(-1);
	public static final ConfigOption<Integer> REQUEST_FRAME_MAX =
			ConfigOptions.key("requestFrameMax".toLowerCase()).defaultValue(-1);
	public static final ConfigOption<Integer> REQUEST_HEART_BEAT =
			ConfigOptions.key("requestHeartBeat".toLowerCase()).defaultValue(-1);

	public static RMQConnectionConfig getConnectionConfig(TableProperties properties) {
		RMQConnectionConfig.Builder builder = new RMQConnectionConfig.Builder();
		if (properties.contains(URI)) {
			builder.setUri(properties.getString(URI));
		}

		if (properties.contains(HOST)) {
			builder.setHost(properties.getString(HOST));
		}

		if (properties.contains(PORT)) {
			builder.setPort(properties.getInteger(PORT));
		}

		if (properties.contains(VIRTUAL_HOST)) {
			builder.setVirtualHost(properties.getString(VIRTUAL_HOST));
		}

		if (properties.contains(USER_NAME)) {
			builder.setUserName(properties.getString(USER_NAME));
		}

		if (properties.contains(PASSWORD)) {
			builder.setPassword(properties.getString(PASSWORD));
		}

		if (properties.contains(NETWORK_RECOVERY_INTERVAL)) {
			builder.setNetworkRecoveryInterval(properties.getInteger(NETWORK_RECOVERY_INTERVAL));
		}

		if (properties.contains(AUTO_RECOVERY)) {
			builder.setAutomaticRecovery(properties.getBoolean(AUTO_RECOVERY));
		}

		if (properties.contains(TOPOLOGY_RECOVERY)) {
			builder.setTopologyRecoveryEnabled(properties.getBoolean(TOPOLOGY_RECOVERY));
		}

		if (properties.contains(CONNECTION_TIMEOUT)) {
			builder.setConnectionTimeout(properties.getInteger(CONNECTION_TIMEOUT));
		}

		if (properties.contains(REQUEST_CHANNEL_MAX)) {
			builder.setRequestedChannelMax(properties.getInteger(REQUEST_CHANNEL_MAX));
		}
		if (properties.contains(REQUEST_FRAME_MAX)) {
			builder.setRequestedFrameMax(properties.getInteger(REQUEST_FRAME_MAX));
		}
		if (properties.contains(REQUEST_HEART_BEAT)) {
			builder.setRequestedHeartbeat(properties.getInteger(REQUEST_HEART_BEAT));
		}

		return builder.build();
	}
}
