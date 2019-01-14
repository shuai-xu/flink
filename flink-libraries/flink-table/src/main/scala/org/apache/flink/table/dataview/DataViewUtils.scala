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
import org.apache.flink.api.common.functions.Comparator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo, RowTypeInfo}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.dataview._
import org.apache.flink.table.api.types.{DataType, RowType, TypeConverters}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.typeutils._

import scala.collection.mutable

object DataViewUtils {

  /**
    * Use NullSerializer for StateView fields from accumulator type information.
    *
    * @param index index of aggregate function
    * @param aggFun aggregate function
    * @param externalAccType accumulator type information, only support pojo type
    * @param isStateBackedDataViews is data views use state backend
    * @return mapping of accumulator type information and data view config which contains id,
    *         field name and state descriptor
    */
  def useNullSerializerForStateViewFieldsFromAccType(
    index: Int,
    aggFun: AggregateFunction[_, _],
    externalAccType: DataType,
    isStateBackedDataViews: Boolean)
  : (DataType, Array[DataViewSpec]) = {

    val externalAccTypeInfo = TypeConverters.createExternalTypeInfoFromDataType(externalAccType)
    val acc = aggFun.createAccumulator()
    val accumulatorSpecs = new mutable.ArrayBuffer[DataViewSpec]

    externalAccTypeInfo match {
      case pojoType: PojoTypeInfo[_] if pojoType.getArity > 0 =>
        val arity = pojoType.getArity
        val newPojoFields = new util.ArrayList[PojoField]()

        for (i <- 0 until arity) {
          val pojoField = pojoType.getPojoFieldAt(i)
          val field = pojoField.getField
          val fieldName = field.getName
          field.setAccessible(true)
          val instance = field.get(acc)
          val (newTypeInfo: TypeInformation[_], spec: Option[DataViewSpec]) =
            decorateDataViewTypeInfo(
              pojoField.getTypeInformation,
              instance,
              isStateBackedDataViews,
              index,
              i,
              fieldName)

          newPojoFields.add(new PojoField(field, newTypeInfo))
          if (spec.isDefined) {
            accumulatorSpecs += spec.get
          }
        }
        (new PojoTypeInfo(externalAccTypeInfo.getTypeClass, newPojoFields),
            accumulatorSpecs.toArray)

      // RowType's ExternalTypeInfo is RowTypeInfo
      // so we add another check => acc.isInstanceOf[GenericRow]
      case r: RowTypeInfo if acc.isInstanceOf[GenericRow] =>
        val accInstance = acc.asInstanceOf[GenericRow]
        val (arity, fieldNames, fieldTypes) = (r.getArity, r.getFieldNames, r.getFieldTypes)
        val newFieldTypes = for (i <- 0 until arity) yield {
          val fieldName = fieldNames(i)
          val fieldInstance = accInstance.getField(i)
          val (newTypeInfo: TypeInformation[_], spec: Option[DataViewSpec]) =
            decorateDataViewTypeInfo(
              fieldTypes(i),
              fieldInstance,
              isStateBackedDataViews,
              index,
              i,
              fieldName)
          if (spec.isDefined) {
            accumulatorSpecs += spec.get
          }
          newTypeInfo
        }

        val newType = new RowType(classOf[GenericRow], newFieldTypes, fieldNames)
        (newType, accumulatorSpecs.toArray)
      case ct: CompositeType[_] if includesDataView(ct) =>
        throw new TableException(
          "MapView, SortedMapView and ListView only supported in accumulators of POJO type.")
      case _ => (externalAccType, Array.empty)
    }
  }

  /** Recursively checks if composite type includes a data view type. */
  def includesDataView(ct: CompositeType[_]): Boolean = {
    (0 until ct.getArity).exists(i =>
      ct.getTypeAt(i) match {
        case nestedCT: CompositeType[_] => includesDataView(nestedCT)
        case t: TypeInformation[_] if t.getTypeClass == classOf[ListView[_]] => true
        case t: TypeInformation[_] if t.getTypeClass == classOf[MapView[_, _]] => true
        case t: TypeInformation[_] if t.getTypeClass == classOf[SortedMapView[_, _]] => true
        case _ => false
      }
    )
  }

  /** Analyse dataview element types and decorate the dataview typeinfos */
  def decorateDataViewTypeInfo(
    info: TypeInformation[_],
    instance: AnyRef,
    isStateBackedDataViews: Boolean,
    aggIndex: Int,
    fieldIndex: Int,
    fieldName: String
  ): (TypeInformation[_], Option[DataViewSpec])  = {
    var spec: Option[DataViewSpec] = None
    val resultTypeInfo: TypeInformation[_] = info match {
      case ct: CompositeType[_] if includesDataView(ct) =>
        throw new TableException(
          "MapView, SortedMapView and ListView only supported at first level of " +
            "accumulators of Pojo type.")
      case map: MapViewTypeInfo[_, _] =>
        val mapView = instance.asInstanceOf[MapView[_, _]]
        val newTypeInfo = if (mapView != null && mapView.keyType != null &&
          mapView.valueType != null) {

          // use external type for DataView of udaf.
          // todo support analysis the DataView generic class of udaf.
          new MapViewTypeInfo(
            TypeConverters.createExternalTypeInfoFromDataType(mapView.keyType),
            TypeConverters.createExternalTypeInfoFromDataType(mapView.valueType))
        } else {
          map
        }

        if (!isStateBackedDataViews) {
          // add data view field if it is not backed by a state backend.
          // data view fields which are backed by state backend are not serialized.
          newTypeInfo.nullSerializer = false
        } else {
          newTypeInfo.nullSerializer = true

          // create map view specs with unique id (used as state name)
          spec = Some(MapViewSpec(
            "agg" + aggIndex + "$" + fieldName,
            fieldIndex, // dataview field index in pojo
            newTypeInfo))
        }
        newTypeInfo

        // SortedMapView must be initialized
      case map: SortedMapViewTypeInfo[_, _] if instance != null =>
        val sortedMapView = instance.asInstanceOf[SortedMapView[_, _]]
        val (keyType, valueType) = if (sortedMapView.keyType != null
          && sortedMapView.valueType != null) {
          (sortedMapView.keyType, sortedMapView.valueType)
        } else {
          (TypeConverters.createInternalTypeFromTypeInfo(map.keyType),
              TypeConverters.createInternalTypeFromTypeInfo(map.valueType))
        }

        // todo support analysis the DataView generic class of udaf.
        val newTypeInfo = if (isStateBackedDataViews) {
          new SortedMapViewTypeInfo(
            OrderedTypeUtils.createComparatorFromDataType(
              keyType, sortedMapView.ord).asInstanceOf[Comparator[Any]],
            OrderedTypeUtils.createOrderedTypeInfoFromDataType(
              keyType, sortedMapView.ord).asInstanceOf[TypeInformation[Any]],
            valueType)
        } else {
          new SortedMapViewTypeInfo(
            new SortedMapViewTypeInfo.ComparableComparator(), keyType, valueType)
        }

        if (!isStateBackedDataViews) {
          // add data view field if it is not backed by a state backend.
          // data view fields which are backed by state backend are not serialized.
          newTypeInfo.nullSerializer = false
        } else {
          newTypeInfo.nullSerializer = true
          // create sorted map view specs with unique id (used as state name)
          spec = Some(SortedMapViewSpec(
            "agg" + aggIndex + "$" + fieldName,
            fieldIndex, // dataview field index in pojo
            newTypeInfo))
        }
        newTypeInfo

      case list: ListViewTypeInfo[_] =>
        val listView = instance.asInstanceOf[ListView[_]]
        val newTypeInfo = if (listView != null && listView.elementType != null) {
          // todo support analysis the DataView generic class of udaf.
          new ListViewTypeInfo(
            TypeConverters.createExternalTypeInfoFromDataType(listView.elementType))
        } else {
          list
        }
        if (!isStateBackedDataViews) {
          // add data view field if it is not backed by a state backend.
          // data view fields which are backed by state backend are not serialized.
          newTypeInfo.nullSerializer = false
        } else {
          newTypeInfo.nullSerializer = true

          // create list view specs with unique is (used as state name)
          spec = Some(ListViewSpec(
            "agg" + aggIndex + "$" + fieldName,
            fieldIndex, // dataview field index in pojo
            newTypeInfo))
        }
        newTypeInfo

      case t: TypeInformation[_] => t
    }

    (resultTypeInfo, spec)
  }

}
