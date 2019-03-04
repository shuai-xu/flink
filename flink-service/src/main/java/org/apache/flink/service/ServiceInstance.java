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

package org.apache.flink.service;

import java.io.Serializable;
import java.util.Objects;

/**
 * A simple Pojo keeps record of one service instance.
 */
public class ServiceInstance implements Serializable {

	private final int instanceId;

	private final String serviceIp;

	private final int servicePort;

	public ServiceInstance(int instanceId, String serviceIp, int servicePort) {
		this.instanceId = instanceId;
		this.serviceIp = serviceIp;
		this.servicePort = servicePort;
	}

	public int getInstanceId() {
		return instanceId;
	}

	public String getServiceIp() {
		return serviceIp;
	}

	public int getServicePort() {
		return servicePort;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ServiceInstance that = (ServiceInstance) o;
		return instanceId == that.instanceId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(instanceId);
	}

	@Override
	public String toString() {
		return "ServiceInstance{" +
			"instanceId=" + instanceId +
			", serviceIp='" + serviceIp + '\'' +
			", servicePort=" + servicePort +
			'}';
	}
}
