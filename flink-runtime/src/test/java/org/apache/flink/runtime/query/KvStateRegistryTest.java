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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapInternalStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link KvStateRegistry}.
 */
public class KvStateRegistryTest extends TestLogger {

	/**
	 * Tests that {@link KvStateRegistryListener} only receive the notifications which
	 * are destined for them.
	 */
	@Test
	public void testKvStateRegistryListenerNotification() throws Exception {
		final JobID jobId1 = new JobID();
		final JobID jobId2 = new JobID();

		final KvStateRegistry kvStateRegistry = new KvStateRegistry();

		final ArrayDeque<JobID> registeredNotifications1 = new ArrayDeque<>(2);
		final ArrayDeque<JobID> deregisteredNotifications1 = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener listener1 = new TestingKvStateRegistryListener(
			registeredNotifications1,
			deregisteredNotifications1);

		final ArrayDeque<JobID> registeredNotifications2 = new ArrayDeque<>(2);
		final ArrayDeque<JobID> deregisteredNotifications2 = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener listener2 = new TestingKvStateRegistryListener(
			registeredNotifications2,
			deregisteredNotifications2);

		kvStateRegistry.registerListener(jobId1, listener1);
		kvStateRegistry.registerListener(jobId2, listener2);

		final JobVertexID jobVertexId = new JobVertexID();
		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
		final String registrationName = "foobar";
		HeapInternalStateBackend heapBackend = new HeapInternalStateBackend(
			2,
			keyGroupRange,
			Thread.currentThread().getContextClassLoader(),
			TestLocalRecoveryConfig.disabled(),
			kvStateRegistry.createTaskRegistry(new JobID(), new JobVertexID()));
		KeyedValueStateDescriptor<Integer, String> desc = new KeyedValueStateDescriptor<>("any", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
		desc.setQueryable("foobar");
		KeyedValueState<Integer, String> state = heapBackend.getKeyedState(desc);

		final KvStateID kvStateID = kvStateRegistry.registerKvState(
			jobId1,
			jobVertexId,
			keyGroupRange,
			registrationName,
			state);

		assertThat(registeredNotifications1.poll(), equalTo(jobId1));
		assertThat(registeredNotifications2.isEmpty(), is(true));

		final JobVertexID jobVertexId2 = new JobVertexID();
		final KeyGroupRange keyGroupRange2 = new KeyGroupRange(0, 1);
		final String registrationName2 = "barfoo";
		final KvStateID kvStateID2 = kvStateRegistry.registerKvState(
			jobId2,
			jobVertexId2,
			keyGroupRange2,
			registrationName2,
			state);

		assertThat(registeredNotifications2.poll(), equalTo(jobId2));
		assertThat(registeredNotifications1.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId1,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateID);

		assertThat(deregisteredNotifications1.poll(), equalTo(jobId1));
		assertThat(deregisteredNotifications2.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId2,
			jobVertexId2,
			keyGroupRange2,
			registrationName2,
			kvStateID2);

		assertThat(deregisteredNotifications2.poll(), equalTo(jobId2));
		assertThat(deregisteredNotifications1.isEmpty(), is(true));
	}

	/**
	 * Tests that {@link KvStateRegistryListener} registered under {@link HighAvailabilityServices#DEFAULT_JOB_ID}
	 * will be used for all notifications.
	 */
	@Test
	public void testLegacyCodePathPreference() throws Exception {
		final KvStateRegistry kvStateRegistry = new KvStateRegistry();
		final ArrayDeque<JobID> stateRegistrationNotifications = new ArrayDeque<>(2);
		final ArrayDeque<JobID> stateDeregistrationNotifications = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener testingListener = new TestingKvStateRegistryListener(
			stateRegistrationNotifications,
			stateDeregistrationNotifications);

		final ArrayDeque<JobID> anotherQueue = new ArrayDeque<>(2);
		final TestingKvStateRegistryListener anotherListener = new TestingKvStateRegistryListener(
			anotherQueue,
			anotherQueue);

		final JobID jobId = new JobID();

		kvStateRegistry.registerListener(HighAvailabilityServices.DEFAULT_JOB_ID, testingListener);
		kvStateRegistry.registerListener(jobId, anotherListener);

		final JobVertexID jobVertexId = new JobVertexID();

		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
		final String registrationName = "registrationName";

		HeapInternalStateBackend heapBackend = new HeapInternalStateBackend(
			2,
			keyGroupRange,
			Thread.currentThread().getContextClassLoader(),
			TestLocalRecoveryConfig.disabled(),
			kvStateRegistry.createTaskRegistry(jobId, new JobVertexID()));

		KeyedValueStateDescriptor<Integer, String> desc = new KeyedValueStateDescriptor<>("any", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
		desc.setQueryable(registrationName);
		KeyedValueState<Integer, String> state = heapBackend.getKeyedState(desc);

		final KvStateID kvStateID = kvStateRegistry.registerKvState(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			state);

		assertThat(stateRegistrationNotifications.poll(), equalTo(jobId));
		// another listener should not have received any notifications
		assertThat(anotherQueue.isEmpty(), is(true));

		kvStateRegistry.unregisterKvState(
			jobId,
			jobVertexId,
			keyGroupRange,
			registrationName,
			kvStateID);

		assertThat(stateDeregistrationNotifications.poll(), equalTo(jobId));
		// another listener should not have received any notifications
		assertThat(anotherQueue.isEmpty(), is(true));
	}

	/**
	 * Testing implementation of {@link KvStateRegistryListener}.
	 */
	private static final class TestingKvStateRegistryListener implements KvStateRegistryListener {

		private final Queue<JobID> stateRegisteredNotifications;
		private final Queue<JobID> stateDeregisteredNotifications;

		private TestingKvStateRegistryListener(
				Queue<JobID> stateRegisteredNotifications,
				Queue<JobID> stateDeregisteredNotifications) {
			this.stateRegisteredNotifications = stateRegisteredNotifications;
			this.stateDeregisteredNotifications = stateDeregisteredNotifications;
		}

		@Override
		public void notifyKvStateRegistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId) {
			stateRegisteredNotifications.offer(jobId);
		}

		@Override
		public void notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) {
			stateDeregisteredNotifications.offer(jobId);
		}
	}

	/**
	 * Testing implementation of {@link InternalKvState}.
	 */
	private static class DummyKvState implements InternalKvState<Integer, VoidNamespace, String> {

		@Override
		public TypeSerializer<Integer> getKeySerializer() {
			return IntSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer<VoidNamespace> getNamespaceSerializer() {
			return VoidNamespaceSerializer.INSTANCE;
		}

		@Override
		public TypeSerializer<String> getValueSerializer() {
			return new DeepCopyingStringSerializer();
		}

		@Override
		public void setCurrentNamespace(VoidNamespace namespace) {
			// noop
		}

		@Override
		public byte[] getSerializedValue(
				final byte[] serializedKeyAndNamespace,
				final TypeSerializer<Integer> safeKeySerializer,
				final TypeSerializer<VoidNamespace> safeNamespaceSerializer,
				final TypeSerializer<String> safeValueSerializer) throws Exception {
			return serializedKeyAndNamespace;
		}

		@Override
		public void clear() {
			// noop
		}
	}

	/**
	 * A dummy serializer that just returns another instance when .duplicate().
	 */
	private static class DeepCopyingStringSerializer extends TypeSerializer<String> {

		private static final long serialVersionUID = -3744051158625555607L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<String> duplicate() {
			return new DeepCopyingStringSerializer();
		}

		@Override
		public String createInstance() {
			return null;
		}

		@Override
		public String copy(String from) {
			return null;
		}

		@Override
		public String copy(String from, String reuse) {
			return null;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(String record, DataOutputView target) throws IOException {

		}

		@Override
		public String deserialize(DataInputView source) throws IOException {
			return null;
		}

		@Override
		public String deserialize(String reuse, DataInputView source) throws IOException {
			return null;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {

		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof DeepCopyingStringSerializer;
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return null;
		}

		@Override
		public CompatibilityResult<String> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			return null;
		}
	}
}
