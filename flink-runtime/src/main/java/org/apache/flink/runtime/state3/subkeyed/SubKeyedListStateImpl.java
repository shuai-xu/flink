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

package org.apache.flink.runtime.state3.subkeyed;

import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state3.StateStorage;
import org.apache.flink.runtime.state3.heap.HeapStateStorage;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link SubKeyedListState} backed by a state storage.
 * The pairs in the storage are formatted as {(K, N) -> List{E}}.
 * Because the pairs are partitioned by K, all the elements under the same key
 * reside in the same group. They can be easily retrieved with a prefix iterator
 * on the key.
 *
 * @param <K> Type of the keys in the state.
 * @param <N> Type of the namespaces in the state.
 * @param <E> Type of the elements in the state.
 */
public final class SubKeyedListStateImpl<K, N, E> implements SubKeyedListState<K, N, E> {

	/**
	 * The descriptor of this state.
	 */
	private final SubKeyedListStateDescriptor descriptor;

	/**
	 * The state storage where the values are stored.
	 */
	private final StateStorage stateStorage;

	//--------------------------------------------------------------------------

	/**
	 * Constructor with the state storage to store elements.
	 *
	 * @param stateStorage The state storage where elements are stored.
	 */
	public SubKeyedListStateImpl(
		SubKeyedListStateDescriptor descriptor,
		StateStorage stateStorage
	) {
		this.descriptor = Preconditions.checkNotNull(descriptor);
		this.stateStorage = Preconditions.checkNotNull(stateStorage);
	}

	@Override
	public SubKeyedListStateDescriptor getDescriptor() {
		return descriptor;
	}

	//--------------------------------------------------------------------------

	@Override
	public boolean contains(K key, N namespace) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				return getOrDefault(key, namespace, null) != null;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public List<E> get(K key, N namespace) {
		return getOrDefault(key, namespace, null);
	}

	@Override
	public List<E> getOrDefault(K key, N namespace, List<E> defaultList) {
		if (key == null || namespace == null) {
			return defaultList;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				return value == null ? defaultList : value;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<N, List<E>> getAll(K key) {
		if (key == null) {
			return Collections.emptyMap();
		}

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).getAll(key);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void add(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> list = (List<E>) heapStateStorage.get(key);
				if (list == null) {
					list = new ArrayList<>();
					heapStateStorage.put(key, list);
				}
				list.add(element);
			} else {
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void addAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.transform(key, elements, (previousState, value) -> {
					if (previousState == null) {
						previousState = new ArrayList<>();
					}
					for (E v : elements) {
						Preconditions.checkNotNull(v, "You cannot add null to a ListState.");
						((List<E>) previousState).add(v);
					}
					return previousState;
				});
			} else {
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void put(K key, N namespace, E element) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(element);

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.put(key, Arrays.asList(element));
			} else {
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void putAll(K key, N namespace, Collection<? extends E> elements) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(namespace);
		Preconditions.checkNotNull(elements);

		if (elements.isEmpty()) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> list = new ArrayList<>();
				for (E element : elements) {
					Preconditions.checkNotNull(element, "You cannot add null to a ListState.");
					list.add(element);
				}
				heapStateStorage.put(key, list);
			} else {
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void remove(K key, N namespace) {
		if (key == null || namespace == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				heapStateStorage.remove(key);
			} else {
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean remove(K key, N namespace, E elementToRemove) {
		if (key == null || namespace == null) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				boolean success = false;
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				if (value != null) {
					success = value.remove(elementToRemove);
					if (value.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
				return success;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public boolean removeAll(K key, N namespace, Collection<? extends E> elements) {
		if (key == null || namespace == null || elements == null || elements.isEmpty()) {
			return false;
		}

		try {
			if (stateStorage.lazySerde()) {
				boolean success = false;
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);
				if (value != null) {
					success = value.removeAll(elements);
					if (value.isEmpty()) {
						heapStateStorage.remove(key);
					}
				}
				return success;
			} else {
				return false;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public void removeAll(K key) {
		if (key == null) {
			return;
		}

		try {
			if (stateStorage.lazySerde()) {
				((HeapStateStorage) stateStorage).removeAll(key);
			} else {

			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public Iterator<N> iterator(K key) {
		Preconditions.checkNotNull(key);

		try {
			if (stateStorage.lazySerde()) {
				return ((HeapStateStorage) stateStorage).namespaceIterator(key);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public E poll(K key, N namespace) {
		try {
			if (stateStorage.lazySerde()) {
				HeapStateStorage heapStateStorage = (HeapStateStorage) stateStorage;
				heapStateStorage.setCurrentNamespace(namespace);
				List<E> value = (List<E>) heapStateStorage.get(key);

				if (value == null) {
					return null;
				}

				E element = value.remove(0);

				if (value.isEmpty()) {
					heapStateStorage.remove(key);
				}

				return element;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

	@Override
	public E peek(K key, N namespace) {
		try {
			if (stateStorage.lazySerde()) {
				List<E> list = get(key, namespace);
				return list == null ? null : list.get(0);
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new StateAccessException(e);
		}
	}

}

