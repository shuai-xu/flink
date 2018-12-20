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

package org.apache.flink.streaming.connectors.hive;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class used to serialize to and from raw hdfs file type.
 * Highly inspired by HCatRecordSerDe (almost copied from this class)in hive-catalog-core.
 */
public class HiveRecordSerDe {
	/**
	 * Return underlying Java Object from an object-representation
	 * that is readable by a provided ObjectInspector.
	 */
	public static Object serializeField(Object field, ObjectInspector fieldObjectInspector)
			throws SerDeException {
		Object res;
		if (fieldObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
			res = serializePrimitiveField(field, fieldObjectInspector);
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.STRUCT) {
			res = serializeStruct(field, (StructObjectInspector) fieldObjectInspector);
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.LIST) {
			res = serializeList(field, (ListObjectInspector) fieldObjectInspector);
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.MAP) {
			res = serializeMap(field, (MapObjectInspector) fieldObjectInspector);
		} else {
			throw new SerDeException(HiveRecordSerDe.class.toString()
									+ " does not know what to do with fields of unknown category: "
									+ fieldObjectInspector.getCategory() + " , type: " + fieldObjectInspector.getTypeName());
		}
		return res;
	}

	/**
	 * Helper method to return underlying Java Map from
	 * an object-representation that is readable by a provided
	 * MapObjectInspector.
	 */
	private static Map<?, ?> serializeMap(Object f, MapObjectInspector moi) throws SerDeException {
		ObjectInspector koi = moi.getMapKeyObjectInspector();
		ObjectInspector voi = moi.getMapValueObjectInspector();
		Map<Object, Object> m = new HashMap<Object, Object>();

		Map<?, ?> readMap = moi.getMap(f);
		if (readMap == null) {
			return null;
		} else {
			for (Map.Entry<?, ?> entry : readMap.entrySet()) {
				m.put(serializeField(entry.getKey(), koi), serializeField(entry.getValue(), voi));
			}
		}
		return m;
	}

	private static List<?> serializeList(Object f, ListObjectInspector loi) throws SerDeException {
		List l = loi.getList(f);
		if (l == null) {
			return null;
		}

		ObjectInspector eloi = loi.getListElementObjectInspector();
		if (eloi.getCategory() == ObjectInspector.Category.PRIMITIVE) {
			List<Object> list = new ArrayList<Object>(l.size());
			for (int i = 0; i < l.size(); i++) {
				list.add(((PrimitiveObjectInspector) eloi).getPrimitiveJavaObject(l.get(i)));
			}
			return list;
		} else if (eloi.getCategory() == ObjectInspector.Category.STRUCT) {
			List<List<?>> list = new ArrayList<List<?>>(l.size());
			for (int i = 0; i < l.size(); i++) {
				list.add(serializeStruct(l.get(i), (StructObjectInspector) eloi));
			}
			return list;
		} else if (eloi.getCategory() == ObjectInspector.Category.LIST) {
			List<List<?>> list = new ArrayList<List<?>>(l.size());
			for (int i = 0; i < l.size(); i++) {
				list.add(serializeList(l.get(i), (ListObjectInspector) eloi));
			}
			return list;
		} else if (eloi.getCategory() == ObjectInspector.Category.MAP) {
			List<Map<?, ?>> list = new ArrayList<Map<?, ?>>(l.size());
			for (int i = 0; i < l.size(); i++) {
				list.add(serializeMap(l.get(i), (MapObjectInspector) eloi));
			}
			return list;
		} else {
			throw new SerDeException(HiveRecordSerDe.class.toString()
									+ " does not know what to do with fields of unknown category: "
									+ eloi.getCategory() + " , type: " + eloi.getTypeName());
		}
	}

	// todo: Comparing to original HCatRecordSerDe.java, we may need add more type converter according to conf.
	private static Object serializePrimitiveField(Object field, ObjectInspector fieldObjectInspector) {
		if (field == null) {
			return null;
		}
		Object f = ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveJavaObject(field);
		return f;
	}

	/**
	 * Return serialized HCatRecord from an underlying
	 * object-representation, and readable by an ObjectInspector.
	 * @param obj : Underlying object-representation
	 * @param soi : StructObjectInspector
	 * @return HCatRecord
	 */
	private static List<?> serializeStruct(Object obj, StructObjectInspector soi)
			throws SerDeException {
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		List<Object> list = soi.getStructFieldsDataAsList(obj);

		if (list == null) {
			return null;
		}

		List<Object> l = new ArrayList<Object>(fields.size());

		if (fields != null) {
			for (int i = 0; i < fields.size(); i++) {
				// Get the field objectInspector and the field object.
				ObjectInspector foi = fields.get(i).getFieldObjectInspector();
				Object f = list.get(i);
				Object res = serializeField(f, foi);
				l.add(i, res);
			}
		}
		return l;
	}

}
