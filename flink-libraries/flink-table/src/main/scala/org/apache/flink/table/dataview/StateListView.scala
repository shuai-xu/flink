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
package org.apache.flink.table.dataview

import java.util
import java.lang.{Iterable => JIterable}

import org.apache.flink.runtime.state2.subkeyed.SubKeyedListState
import org.apache.flink.runtime.state2.keyed.KeyedListState
import org.apache.flink.table.api.dataview.ListView

class SubKeyedStateListView[K, N, T](state: SubKeyedListState[K, N, T]) extends ListView[T] {
  private var key: K = _
  private var namespace: N = _

  def setKeyNamespace(key: K, namespace: N): Unit = {
    this.key = key
    this.namespace = namespace
  }

  override def get: JIterable[T] = state.get(key, namespace)

  override def add(value: T): Unit = state.add(key, namespace, value)

  override def addAll(list: util.List[T]): Unit = state.addAll(key, namespace, list)

  override def clear(): Unit = state.remove(key, namespace)
}

/**
  * used for minibatch
  * @param state
  * @tparam K
  * @tparam E
  */
class KeyedStateListView[K, E](state: KeyedListState[K, E])
  extends ListView[E] {

  protected var stateKey: K = null.asInstanceOf[K]

  def setKey(key: K) = this.stateKey = key

  override def get: JIterable[E] = {
    state.get(stateKey)
  }

  override def add(value: E): Unit = {
    state.add(stateKey, value)
  }

  override def addAll(list: util.List[E]): Unit = {
    state.addAll(stateKey, list)
  }

  override def remove(value: E): Boolean = {
    val list = this.get.asInstanceOf[util.List[E]]
    if (list != null && list.remove(value)) {
      this.clear()
      this.addAll(list)
      true
    } else {
      false
    }
  }

  override def clear(): Unit = {
    state.remove(stateKey)
  }

}
