# ###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from threading import RLock

from py4j.java_collections import ListConverter, MapConverter

from pyflink.java_gateway import get_gateway
from pyflink.sql.data_type import *
# from pyflink.data_types import *

# the mapping of python sql types to java sql types
_sql_basic_types_py2j_map = None
_sql_complex_type_py2j_map = None
_sql_basic_types_j2py_map = None
_sql_complex_type_j2py_map = None
_init_lock = RLock()


class TypesUtil(object):

    @staticmethod
    def to_java_map(dict_data):
        _gateway = get_gateway()
        return MapConverter().convert(dict_data, _gateway._gateway_client)

    @staticmethod
    def to_java_sql_type(py_sql_type):
        global _sql_basic_types_py2j_map
        global _sql_complex_type_py2j_map
        if (_sql_basic_types_py2j_map is None) or (_sql_complex_type_py2j_map is None):
            with _init_lock:
                _gateway = get_gateway()
                j_sql_types = _gateway.jvm.org.apache.flink.table.types.DataTypes
                j_sql_decimal = _gateway.jvm.org.apache.flink.table.api.types.DECIMAL
                _sql_basic_types_py2j_map = {
                    DataTypes.STRING: j_sql_types.STRING,
                    DataTypes.INT: j_sql_types.INT,
                    DataTypes.BOOLEAN: j_sql_types.BOOLEAN,
                    DataTypes.DOUBLE: j_sql_types.DOUBLE,
                    DataTypes.FLOAT: j_sql_types.FLOAT,
                    DataTypes.BYTE: j_sql_types.BYTE,
                    DataTypes.LONG: j_sql_types.LONG,
                    DataTypes.SHORT: j_sql_types.SHORT,
                    DataTypes.CHAR: j_sql_types.CHAR,
                    DataTypes.BYTE_ARRARY: j_sql_types.BYTE_ARRAY,
                    DataTypes.DATE: j_sql_types.DATE,
                    DataTypes.TIME: j_sql_types.TIME,
                    DataTypes.TIMESTAMP: j_sql_types.TIMESTAMP,
                    DataTypes.ROWTIME_INDICATOR: j_sql_types.ROWTIME_INDICATOR,
                    DataTypes.PROCTIME_INDICATOR: j_sql_types.PROCTIME_INDICATOR
                    # TODO: interval...
                }

                _sql_complex_type_py2j_map = {
                    DecimalType: j_sql_decimal,
                    RowType: _gateway.jvm.org.apache.flink.table.types.RowType
                }

        # TODO: other complex types
        if isinstance(py_sql_type, DecimalType):
            j_clz = _sql_complex_type_py2j_map.get(type(py_sql_type))
            return j_clz(py_sql_type.precision, py_sql_type.scale)
        if isinstance(py_sql_type, RowType):
            j_types = [_sql_basic_types_py2j_map[pt] for pt in py_sql_type.data_types]
            j_types_arr = TypesUtil._convert_py_list_to_java_array(
                'org.apache.flink.table.types.DataType',
                j_types
            )
            j_names_arr = TypesUtil._convert_py_list_to_java_array(
                'java.lang.String',
                py_sql_type.fields_names
            )
            j_clz = _sql_complex_type_py2j_map.get(type(py_sql_type))
            return j_clz(j_types_arr, j_names_arr)

        return _sql_basic_types_py2j_map.get(py_sql_type)

    @staticmethod
    def to_py_sql_type(j_sql_type):
        global _sql_basic_types_j2py_map
        global _sql_complex_type_j2py_map
        if (_sql_basic_types_j2py_map is None) or (_sql_complex_type_j2py_map is None):
            with _init_lock:
                _gateway = get_gateway()
                j_data_types = _gateway.jvm.org.apache.flink.table.types.DataTypes
                # j_sql_decimal = _gateway.jvm.org.apache.flink.table.api.types.DECIMAL
                _sql_basic_types_j2py_map = {
                    j_data_types.STRING: DataTypes.STRING,
                    j_data_types.INT: DataTypes.INT,
                    j_data_types.BOOLEAN: DataTypes.BOOLEAN,
                    j_data_types.DOUBLE: DataTypes.DOUBLE,
                    j_data_types.FLOAT: DataTypes.FLOAT,
                    j_data_types.BYTE: DataTypes.BYTE,
                    j_data_types.LONG: DataTypes.LONG,
                    j_data_types.SHORT: DataTypes.SHORT,
                    j_data_types.CHAR: DataTypes.CHAR,
                    j_data_types.BYTE_ARRAY: DataTypes.BYTE_ARRARY,
                    j_data_types.DATE: DataTypes.DATE,
                    j_data_types.TIMESTAMP: DataTypes.TIMESTAMP,
                    j_data_types.PROCTIME_INDICATOR: DataTypes.PROCTIME_INDICATOR,
                    j_data_types.ROWTIME_INDICATOR: DataTypes.ROWTIME_INDICATOR,
                    j_data_types.TIME: DataTypes.TIME
                }

                _sql_complex_type_j2py_map = {
                    # j_sql_decimal: DecimalType
                }

        # DecimalType: j_sql_decimal(py_sql_type.precision, py_sql_type.scale)
        # TODO: other complex types
        # if isinstance(py_sql_type, DecimalType):
        #     j_clz = _sql_complex_type_py2j_map.get(type(py_sql_type))
        #     return j_clz(py_sql_type.precision, py_sql_type.scale)
        return _sql_basic_types_j2py_map.get(j_sql_type)

    @staticmethod
    def jarray_to_pylist(jarr):
        """
        :type jarr: JavaArray
        :param jarr: Py4j's JavaArray

        :rtype: list
        :return:
        """
        size = len(jarr)
        return [jarr[i] for i in xrange(size)]

    @staticmethod
    def _convert_to_java_type(py_type):
        """
        Convert python API types to 'Java Type'(py4j).
        Note: for internal use
        :param py_type:
        :return:
        """
        _gateway = get_gateway()
        types = _gateway.jvm.org.apache.flink.api.common.typeinfo.Types

        return {
            StringType: types.STRING,
            IntegerType: types.INT,
            BooleanType: types.BOOLEAN,
            DoubleType: types.DOUBLE,
            FloatType: types.FLOAT,
            ByteType: types.BYTE,
            LongType: types.LONG,
            ShortType: types.SHORT,
            CharType: types.CHAR,
            BinaryType: types.PRIMITIVE_ARRAY(types.BYTE),
            DateType: types.SQL_DATE,
            TimestampType: types.SQL_TIMESTAMP,
            TimeType: types.SQL_TIME   #,
            #DecimalType: types.DECIMAL
            # TODO: other types
        }.get(type(py_type), types.STRING)

        # TODO: to support Types.ROW

    @staticmethod
    def _convert_pylist_to_java_list(py_list):
        _gateway = get_gateway()
        j_list = ListConverter().convert(
            py_list, _gateway._gateway_client)
        return j_list

    @staticmethod
    def _convert_tuple_list(tuple_list):
        _gateway = get_gateway()
        java_tuple_list = []
        for item in tuple_list:
            java_tuple_list.append(TypesUtil._tuple_to_java_tuple(item))
        return ListConverter().convert(
            java_tuple_list, _gateway._gateway_client)

    @staticmethod
    def _tuple_to_java_tuple(data):
        _gateway = get_gateway()
        size = len(data)
        java_tuple = getattr(_gateway.jvm.org.apache.flink.api.java.tuple,
                             "Tuple" + str(size))
        return java_tuple(*list(data))

    @staticmethod
    def _java_tuple_to_tuple(flink_tuple, types=None):
        """

        :param flink_tuple:
        :return:
        """
        # _gateway = get_gateway()
        arity = flink_tuple.getArity()

        if types is None:
            # for primitive types, using Py4j's default type mapping
            tmp_list = []
            for i in range(arity):
                tmp_list.append(flink_tuple.getField(i))
            return tuple(tmp_list)
        else:
            # specified type info
            tmp_list = []
            for i in range(arity):
                py_instance = types[i](flink_tuple.getField(i))
                tmp_list.append(py_instance)
            return tuple(tmp_list)

    @staticmethod
    def _convert_py_list_to_java_array(type_info, seq):
        _gateway = get_gateway()
        ns = _gateway.jvm

        # e.g.: org.apache.flink.streaming.api.TimeCharacteristic
        attrs = type_info.split('.')
        for attr in attrs:
            if hasattr(ns, attr):
                ns = getattr(ns, attr)
            else:
                raise Exception('Has no such attribute')
        size = len(seq)
        java_array = _gateway.new_array(ns, size)
        for i in range(size):
            java_array[i] = seq[i]

        return java_array

    @staticmethod
    def class_for_name(class_name):
        _gateway = get_gateway()
        clz = _gateway.jvm

        # e.g.: org.apache.flink.streaming.api.TimeCharacteristic
        attrs = class_name.split('.')
        for attr in attrs:
            if hasattr(clz, attr):
                clz = getattr(clz, attr)
            else:
                raise Exception('Has no such attribute')

        return clz

    # @staticmethod
    # def class_for_name(class_name):
    #     ReflectionUtil = get_gateway().jvm.py4j.reflection.ReflectionUtil
    #     java_clazz = ReflectionUtil.classForName(class_name)
    #
    #     _gateway = get_gateway()
    #     clz = _gateway.jvm
    #
    #     # e.g.: org.apache.flink.streaming.api.TimeCharacteristic
    #     attrs = class_name.split('.')
    #     for attr in attrs:
    #         if hasattr(clz, attr):
    #             clz = getattr(clz, attr)
    #         else:
    #             raise Exception('Has no such attribute')
    #
    #     return java_clazz
