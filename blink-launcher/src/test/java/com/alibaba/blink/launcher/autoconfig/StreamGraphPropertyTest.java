/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.blink.launcher.autoconfig;

import com.alibaba.blink.launcher.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class StreamGraphPropertyTest {
	@Test
	public void ensureCompatibility() throws IOException {
		StreamGraphProperty properties = getMockedStreamGraphProperty();

		// verify output parse to the same object
		assertEquals(properties, StreamGraphProperty.fromJson(properties.toString()));
		assertEquals(properties.toString(), StreamGraphProperty.fromJson(properties.toString()).toString());

		// verify backward compatibility with current version format
		StreamGraphProperty transformationPropertiesRead =
			StreamGraphProperty.fromJson(TestUtil.getResource("compatibility/supported_transformationprop_format.json"));
		assertEquals(properties.toString(), transformationPropertiesRead.toString());

		// verify output format
		String expectedOutput = TestUtil.getResourceContent("compatibility/normalized_transformationprop_format.json");
		assertEquals(expectedOutput.trim(), properties.toString());
	}

	private StreamGraphProperty getMockedStreamGraphProperty() {
		StreamGraphProperty properties = new StreamGraphProperty();

		// node with non-default values
		StreamNodeProperty node1 = new StreamNodeProperty(1);
		node1.setName("node1");
		node1.setSlotSharingGroup("test");
		node1.setChainingStrategy("HEAD");
		node1.setParallelism(8);
		node1.setMaxParallelism(10240);
		node1.setCpuCores(0.5);
		node1.setHeapMemoryInMB(1024);
		node1.setDirectMemoryInMB(1);
		node1.setNativeMemoryInMB(1);
		node1.setLock(Arrays.asList("heap_memory", "parallelism", "chainingStrategy", "vcore", "native_memory"));
		node1.setGpuLoad(5);
		node1.setWorkDirVssdLoad(2);
		node1.setLogDirVssdLoad(3);
		node1.setStateNativeMemoryInMB(12345);
		node1.setResource("cnn", 1);
		node1.setResource("rnn", 2);
		node1.addResourceConstraint("zk", "3");
		node1.addResourceConstraint("hbase", "1");

		// node with default (unassigned) values
		StreamNodeProperty node2 = new StreamNodeProperty(2);
		node2.setResource("cnn", 3);
		node2.setResource("cnn", 5);
		node2.setLock(Arrays.asList("heap_memory", "parallelism"));

		// node with output of previous explicit unassigned values
		StreamNodeProperty node3 = new StreamNodeProperty(3);
		node3.setName("");
		node3.setSlotSharingGroup("default");
		node3.setChainingStrategy("ALWAYS");
		node3.setParallelism(0);
		node3.setMaxParallelism(1);
		node3.setCpuCores(0);
		node3.setHeapMemoryInMB(0);
		node3.setDirectMemoryInMB(0);
		node3.setNativeMemoryInMB(0);
		node3.setGpuLoad(0);
		node3.setWorkDirVssdLoad(0);
		node3.setLogDirVssdLoad(0);
		node3.setStateNativeMemoryInMB(0);

		// node with special backward compatible values
		StreamNodeProperty node4 = new StreamNodeProperty(4);
		node4.setName("null");
		node4.setSlotSharingGroup("null");
		node4.setChainingStrategy("null");
		node4.setLock(Arrays.asList());

		StreamNodeProperty node5 = new StreamNodeProperty(5);

		node5.setLock(Arrays.asList("chainingStrategy"));

		properties.getStreamNodeProperties().add(node1);
		properties.getStreamNodeProperties().add(node2);
		properties.getStreamNodeProperties().add(node3);
		properties.getStreamNodeProperties().add(node4);
		properties.getStreamNodeProperties().add(node5);
		properties.getStreamNodeProperties().add(new StreamNodeProperty(6).setParallelism(101));
		properties.getStreamNodeProperties().add(new StreamNodeProperty(7).setParallelism(101));

		StreamEdgeProperty link12 = new StreamEdgeProperty(1, 2, 0); // link with non-default values and also with special backward compatible values
		link12.setShipStrategy("null");

		StreamEdgeProperty link13 = new StreamEdgeProperty(1, 3, 0); // link with default (unassigned) values

		StreamEdgeProperty link34 = new StreamEdgeProperty(3, 4, 0); // link with output of previous unassigned values
		link34.setShipStrategy("");

		properties.getStreamEdgeProperties().add(link12);
		properties.getStreamEdgeProperties().add(link13);
		properties.getStreamEdgeProperties().add(link34);
		properties.getStreamEdgeProperties().add(new StreamEdgeProperty(3, 5, 0));
		properties.getStreamEdgeProperties().add(new StreamEdgeProperty(5, 6, 0));
		properties.getStreamEdgeProperties().add(new StreamEdgeProperty(6, 7, 0));
		properties.getStatefulNodes().add(1);
		properties.getStatefulNodes().add(3);
		properties.postUpdate();

		return properties;
	}
}
