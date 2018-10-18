/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.types;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.typeutils.TypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utils for types.
 */
public class DataTypes {

	public static final StringType STRING = StringType.INSTANCE;

	public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

	public static final DoubleType DOUBLE = DoubleType.INSTANCE;

	public static final FloatType FLOAT = FloatType.INSTANCE;

	public static final ByteType BYTE = ByteType.INSTANCE;

	public static final IntType INT = IntType.INSTANCE;

	public static final LongType LONG = LongType.INSTANCE;

	public static final ShortType SHORT = ShortType.INSTANCE;

	public static final CharType CHAR = CharType.INSTANCE;

	public static final ByteArrayType BYTE_ARRAY = ByteArrayType.INSTANCE;

	public static final DateType DATE = DateType.DATE;

	public static final TimestampType TIMESTAMP = TimestampType.TIMESTAMP;

	public static final TimeType TIME = TimeType.INSTANCE;

	public static final DateType INTERVAL_MONTHS = DateType.INTERVAL_MONTHS;

	public static final TimestampType INTERVAL_MILLIS = TimestampType.INTERVAL_MILLIS;

	public static final TimestampType ROWTIME_INDICATOR = TimestampType.ROWTIME_INDICATOR;

	public static final TimestampType PROCTIME_INDICATOR = TimestampType.PROCTIME_INDICATOR;

	public static final IntervalRowsType INTERVAL_ROWS = IntervalRowsType.INSTANCE;

	public static final IntervalRangeType INTERVAL_RANGE = IntervalRangeType.INSTANCE;

	public static final List<PrimitiveType> INTEGRAL_TYPES = Arrays.asList(BYTE, SHORT, INT, LONG);

	public static final List<PrimitiveType> FRACTIONAL_TYPES = Arrays.asList(FLOAT, DOUBLE);

	/**
	 * The special field index indicates that this is a row time field.
	 */
	public static final int ROWTIME_STREAM_MARKER = -1;

	/**
	 * The special field index indicates that this is a proc time field.
	 */
	public static final int PROCTIME_STREAM_MARKER = -2;

	/**
	 * The special field index indicates that this is a row time field.
	 */
	public static final int ROWTIME_BATCH_MARKER = -3;

	/**
	 * The special field index indicates that this is a proc time field.
	 */
	public static final int PROCTIME_BATCH_MARKER = -4;

	public static ArrayType createArrayType(InternalType elementType) {
		return new ArrayType(elementType);
	}

	public static ArrayType createPrimitiveArrayType(InternalType elementType) {
		return new ArrayType(elementType, true);
	}

	public static DecimalType createDecimalType(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static MapType createMapType(InternalType keyType, InternalType valueType) {
		if (keyType == null) {
			throw new IllegalArgumentException("keyType should not be null.");
		}
		if (valueType == null) {
			throw new IllegalArgumentException("valueType should not be null.");
		}
		return new MapType(keyType, valueType);
	}

	public static DataType createArrayType(DataType elementType) {
		return of(ObjectArrayTypeInfo.getInfoFor(TypeUtils.createTypeInfoFromDataType(elementType)));
	}

	public static DataType createMapType(DataType keyType, DataType valueType) {
		return DataTypes.of(new MapTypeInfo<>(
				TypeUtils.createTypeInfoFromDataType(keyType),
				TypeUtils.createTypeInfoFromDataType(valueType)));
	}

	public static <T> GenericType<T> createGenericType(Class<T> cls) {
		return new GenericType<>(cls);
	}

	public static <T> GenericType<T> createGenericType(TypeInformation<T> typeInfo) {
		return new GenericType<>(typeInfo);
	}

	public static MultisetType createMultisetType(InternalType elementType) {
		if (elementType == null) {
			throw new IllegalArgumentException("elementType should not be null.");
		}
		return new MultisetType(elementType);
	}

	public static DataType of(TypeInformation typeInfo) {
		return new TypeInfoWrappedType(typeInfo);
	}

	public static TypeInformation to(DataType type) {
		return TypeUtils.createTypeInfoFromDataType(type);
	}

	public static InternalType internal(TypeInformation typeInfo) {
		if (typeInfo == null) {
			return null;
		}
		return TypeUtils.internalTypeFromTypeInfo(typeInfo);
	}

	public static InternalType internal(DataType type) {
		if (type == null) {
			return null;
		} else if (type instanceof InternalType) {
			return (InternalType) type;
		} else {
			return ((ExternalType) type).toInternalType();
		}
	}

	public static TypeInformation internalTypeInfo(TypeInformation typeInfo) {
		return to(TypeUtils.internalTypeFromTypeInfo(typeInfo));
	}

	public static TypeInformation internalTypeInfo(DataType t) {
		return to(internal(t));
	}

	public static TypeInformation toTypeInfo(DataType t) {
		if (t == null) {
			return null;
		}
		return TypeUtils.createTypeInfoFromDataType(t);
	}

	public static TypeInformation[] toTypeInfos(DataType[] types) {
		TypeInformation<?>[] typeInfos = new TypeInformation[types.length];
		for (int i = 0; i < types.length; i++) {
			typeInfos[i] = DataTypes.toTypeInfo(types[i]);
		}
		return typeInfos;
	}

	public static InternalType[] internalTypes(TypeInformation[] typeInfos) {
		InternalType[] types = new InternalType[typeInfos.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = DataTypes.internal(typeInfos[i]);
		}
		return types;
	}

	public static DataType[] dataTypes(TypeInformation[] typeInfos) {
		DataType[] types = new DataType[typeInfos.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = DataTypes.of(typeInfos[i]);
		}
		return types;
	}

	public static BaseRowType createBaseRowType(InternalType[] types) {
		return new BaseRowType(types);
	}

	public static BaseRowType createBaseRowType(
			InternalType[] types, String[] fieldNames) {
		return new BaseRowType(types, fieldNames);
	}

	public static BaseRowType createBaseRowType(
			Class<?> typeClass, InternalType[] types, String[] fieldNames) {
		return new BaseRowType(typeClass, types, fieldNames);
	}

	public static DataType createRowType(DataType[] types, String[] fieldNames) {
		return new TypeInfoWrappedType(new RowTypeInfo(typeInfos(types), fieldNames));
	}

	public static DataType createRowType(InternalType[] types, String[] fieldNames) {
		return new TypeInfoWrappedType(new RowTypeInfo(typeInfos(types), fieldNames));
	}

	public static DataType createRowType(DataType... types) {
		return new TypeInfoWrappedType(new RowTypeInfo(typeInfos(types)));
	}

	@SuppressWarnings("unchecked")
	public static DataType createTupleType(Class cls, DataType... types) {
		TupleTypeInfo typeInfo = new TupleTypeInfo(cls, typeInfos(types));
		return of(typeInfo);
	}

	@SuppressWarnings("unchecked")
	public static DataType createTupleType(DataType... types) {
		TupleTypeInfo typeInfo = new TupleTypeInfo(typeInfos(types));
		return of(typeInfo);
	}

	public static <T> PojoBuilder pojoBuilder(Class<T> typeClass) {
		return new PojoBuilder<>(typeClass);
	}

	/**
	 * Builder for external pojo type.
	 */
	public static class PojoBuilder<T> {

		private Class<T> typeClass;
		private List<PojoField> fields;

		private PojoBuilder(Class<T> typeClass) {
			this.typeClass = typeClass;
			this.fields = new ArrayList<>();
		}

		public PojoBuilder field(String name, DataType type) throws NoSuchFieldException {
			fields.add(new PojoField(typeClass.getDeclaredField(name), toTypeInfo(type)));
			return this;
		}

		public TypeInfoWrappedType build() {
			PojoTypeInfo<T> typeInfo = new PojoTypeInfo<T>(typeClass, fields);
			return new TypeInfoWrappedType(typeInfo);
		}
	}

	private static TypeInformation[] typeInfos(DataType... types) {
		return Arrays.stream(types).<TypeInformation>map(TypeUtils::createTypeInfoFromDataType)
				.toArray(TypeInformation[]::new);
	}

	public static int getArity(InternalType t) {
		if (t instanceof BaseRowType) {
			return ((BaseRowType) t).getArity();
		}
		return 1;
	}

	private static final Map<PrimitiveType, Set<PrimitiveType>> AUTO_CAST_MAP;
	static {
		Map<PrimitiveType, Set<PrimitiveType>> map = new HashMap<>();
		map.put(DataTypes.BYTE, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				DataTypes.SHORT, DataTypes.INT, DataTypes.LONG,
				DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.CHAR))));
		map.put(DataTypes.SHORT, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				DataTypes.INT, DataTypes.LONG, DataTypes.FLOAT,
				DataTypes.DOUBLE, DataTypes.CHAR))));
		map.put(DataTypes.INT, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				DataTypes.LONG, DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.CHAR))));
		map.put(DataTypes.LONG, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				DataTypes.FLOAT, DataTypes.DOUBLE, DataTypes.CHAR))));
		map.put(DataTypes.FLOAT, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				DataTypes.DOUBLE))));
		AUTO_CAST_MAP = Collections.unmodifiableMap(map);
	}

	public static boolean shouldAutoCastTo(PrimitiveType t, PrimitiveType castTo) {
		Set<PrimitiveType> set = AUTO_CAST_MAP.get(t);
		return set != null && set.contains(castTo);
	}

	public static DataType extractType(Class cls) {
		return of(TypeExtractor.createTypeInfo(cls));
	}
}
