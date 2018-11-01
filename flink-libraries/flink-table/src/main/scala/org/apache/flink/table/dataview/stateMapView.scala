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
import java.util.Map

import org.apache.flink.runtime.state.keyed.KeyedMapState
import org.apache.flink.runtime.state.subkeyed.SubKeyedMapState
import org.apache.flink.table.api.dataview.MapView

/**
  * [[SubKeyedStateMapView]] is a [[SubKeyedMapState]] with [[MapView]] interface which works on
  * window aggregate.
  */
class SubKeyedStateMapView[K, N, MK, MV](state: SubKeyedMapState[K, N, MK, MV])
  extends MapView[MK, MV]
  with StateDataView[K] {

  private var key: K = _
  private var namespace: N = _

  override def setCurrentKey(key: K): Unit = {
    this.key = key
  }

  def setCurrentNamespace(namespace: N): Unit = {
    this.namespace = namespace
  }

  override def get(mapKey: MK): MV = state.get(key, namespace, mapKey)

  override def put(mapKey: MK, mapValue: MV): Unit = state.add(key, namespace, mapKey, mapValue)

  override def putAll(map: util.Map[MK, MV]): Unit = state.addAll(key, namespace, map)

  override def remove(mapKey: MK): Unit = state.remove(key, namespace, mapKey)

  override def contains(mapKey: MK): Boolean = state.contains(key, namespace, mapKey)

  override def entries: JIterable[util.Map.Entry[MK, MV]] = {
    new JIterable[util.Map.Entry[MK, MV]]() {
      override def iterator(): util.Iterator[util.Map.Entry[MK, MV]] =
        state.iterator(key, namespace)
    }
  }

  override def keys: JIterable[MK] = new JIterable[MK]() {
    override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
      val iter: util.Iterator[util.Map.Entry[MK, MV]] = state.iterator(key, namespace)

      override def next(): MK = iter.next().getKey

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def values: JIterable[MV] = new JIterable[MV]() {
    override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
      val iter: util.Iterator[util.Map.Entry[MK, MV]] = state.iterator(key, namespace)

      override def next(): MV = iter.next().getValue

      override def hasNext: Boolean = iter.hasNext
    }
  }

  override def iterator: util.Iterator[util.Map.Entry[MK, MV]] = state.iterator(key, namespace)

  override def clear(): Unit = state.remove(key, namespace)
}

/**
  * [[KeyedStateMapView]] is a [[KeyedMapState]] with [[MapView]] interface which works on
  * group aggregate.
  */
class KeyedStateMapView[K, MK, MV](state: KeyedMapState[K, MK, MV])
  extends MapView[MK, MV]
  with StateDataView[K] {

  private var stateKey: K = null.asInstanceOf[K]

  override def setCurrentKey(key: K): Unit = {
    this.stateKey = key
  }

  override def get(key: MK): MV = {
    state.get(stateKey, key)
  }

  override def put(key: MK, value: MV): Unit = {
    state.add(stateKey, key, value)
  }

  override def putAll(map: util.Map[MK, MV]): Unit = {
    state.addAll(stateKey, map)
  }

  override def remove(key: MK): Unit = {
    state.remove(stateKey, key)
  }

  override def contains(key: MK): Boolean = {
    state.contains(stateKey, key)
  }

  override def entries: JIterable[Map.Entry[MK, MV]] = {
    new JIterable[Map.Entry[MK, MV]] {
      override def iterator(): util.Iterator[Map.Entry[MK, MV]] =
        new util.Iterator[Map.Entry[MK, MV]] {
          val it = state.iterator(stateKey)
          override def next(): Map.Entry[MK, MV] = it.next()
          override def hasNext: Boolean = it.hasNext
        }
    }
  }

  override def keys: JIterable[MK] = {
    new JIterable[MK] {
      override def iterator(): util.Iterator[MK] = new util.Iterator[MK] {
        val it = state.iterator(stateKey)
        override def next(): MK = it.next().getKey
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def values: JIterable[MV] = {
    new JIterable[MV] {
      override def iterator(): util.Iterator[MV] = new util.Iterator[MV] {
        val it = state.iterator(stateKey)
        override def next(): MV = it.next().getValue
        override def hasNext: Boolean = it.hasNext
      }
    }
  }

  override def iterator: util.Iterator[Map.Entry[MK, MV]] = {
    state.iterator(stateKey)
  }

  override def clear(): Unit = state.remove(stateKey)
}

