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

import org.apache.flink.api.common.state2._
import org.apache.flink.table.typeutils.{ListViewTypeInfo, MapViewTypeInfo, SortedMapViewTypeInfo}

/**
  * Data view specification.
  */
trait DataViewSpec {
  def stateId: String
  def fieldIndex: Int
  def fieldName: String
  def toStateDescriptor: StateDescriptor[_, _ <: State[_]]
  def getStateDataViewClass(hasNamespace: Boolean): Class[_]
  def getCreateStateCall(hasNamespace: Boolean): String
}

case class ListViewSpec[T](
    stateId: String,
    fieldIndex: Int,
    fieldName: String,
    listViewTypeInfo: ListViewTypeInfo[T])
  extends DataViewSpec {

  override def toStateDescriptor: StateDescriptor[_, _ <: State[_]] =
    new ListStateDescriptor[T](stateId, listViewTypeInfo.elementType)

  override def getStateDataViewClass(hasNamespace: Boolean): Class[_] = {
    if (hasNamespace) {
      classOf[SubKeyedStateListView[_, _, _]]
    } else {
      classOf[KeyedStateListView[_, _]]
    }
  }

  override def getCreateStateCall(hasNamespace: Boolean): String = {
    if (hasNamespace) {
      "getSubKeyedListState"
    } else {
      "getKeyedListState"
    }
  }
}

case class MapViewSpec[K, V](
    stateId: String,
    fieldIndex: Int,
    fieldName: String,
    mapViewTypeInfo: MapViewTypeInfo[K, V])
  extends DataViewSpec {

  override def toStateDescriptor: StateDescriptor[_, _ <: State[_]] =
    new MapStateDescriptor[K, V](stateId, mapViewTypeInfo.keyType, mapViewTypeInfo.valueType)

  override def getStateDataViewClass(hasNamespace: Boolean): Class[_] = {
    if (hasNamespace) {
      classOf[SubKeyedStateMapView[_, _, _, _]]
    } else {
      classOf[KeyedStateMapView[_, _, _]]
    }
  }

  override def getCreateStateCall(hasNamespace: Boolean): String = {
    if (hasNamespace) {
      "getSubKeyedMapState"
    } else {
      "getKeyedMapState"
    }
  }
}

case class SortedMapViewSpec[K, V](
    stateId: String,
    fieldIndex: Int,
    fieldName: String,
    sortedMapViewTypeInfo: SortedMapViewTypeInfo[K, V])
  extends DataViewSpec {

  override def toStateDescriptor: StateDescriptor[_, _ <: State[_]] =
    new SortedMapStateDescriptor[K, V](
      stateId,
      sortedMapViewTypeInfo.comparator,
      sortedMapViewTypeInfo.keyType,
      sortedMapViewTypeInfo.valueType)

  override def getStateDataViewClass(hasNamespace: Boolean): Class[_] = {
    if (hasNamespace) {
      // not supported yet
      ???
    } else {
      classOf[KeyedStateSortedMapView[_, _, _]]
    }
  }

  override def getCreateStateCall(hasNamespace: Boolean): String = {
    if (hasNamespace) {
      "getSubKeyedSortedMapState"
    } else {
      "getKeyedSortedMapState"
    }
  }
}
