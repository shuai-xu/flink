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
from pyflink.sql.data_type import DataTypes
from pyflink.util.type_util import TypesUtil


class Column(object):
    def __init__(self, name, data_type, is_nullable=True):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable

    def __eq__(self, other):
        if other is None:
            return False
        return (self._name == other.name
                and self._data_type == other.data_type
                and self._is_nullable == other.is_nullable)

    def __ne__(self, other):
        return not self.__eq__(other)


class ComputedColumn(object):
    def __init__(self, name, expression):
        self.name = name
        self.expression = expression

    def __eq__(self, other):
        if other is None:
            return False
        return (self._name == other.name
                and self.expression == other.expression)

    def __ne__(self, other):
        return not self.__eq__(other)


class Watermark(object):
    def __init__(self, name, eventTime, offset):
        self.name = name
        self.eventTime = eventTime
        self.offset = offset

    def __eq__(self, other):
        if other is None:
            return False
        return (self._name == other.name
                and self.eventTime == other.eventTime
                and self.offset == other.offset)

    def __ne__(self, other):
        return not self.__eq__(other)


class TableSchema(object):
    """
    example 1:
    >>> schema1 = TableSchema(['col1','col2'], [StringType(), IntType()])

    example 2:
    >>> builder = TableSchema.Builder()
    >>> builder.column('col1', StringType()).column('col2', IntType())
    >>> schema2 = builder.build()

    """

    def __init__(self, names=None, types=None, nulls=None, j_schema=None):
        # type
        self._j_table_schema = j_schema

        # if user specify the names & types
        if names is not None and types is not None:
            self.columns = TableSchema._validate(names, types, nulls)
            self.primary_keys = None
            self.unique_keys = None
            self.indexes = None
            self.computed_columns = None
            self.watermarks = None

            j_table_schema_cls = TypesUtil.class_for_name('org.apache.flink.table.api.TableSchema')
            j_name_arr = TypesUtil._convert_py_list_to_java_array('java.lang.String', names)
            j_type_arr = TypesUtil._convert_py_list_to_java_array('org.apache.flink.table.types.DataType', types)

            self._j_table_schema = j_table_schema_cls(j_name_arr, j_type_arr)
        if j_schema is not None:
            self._java_to_python()

    def __str__(self):
        if self._j_table_schema is not None:
            return self._j_table_schema.toString()

    def _to_java_obj(self):
        return self._j_table_schema

    @classmethod
    def _validate(cls, names, types, nulls=None):
        if len(names) != len(types):
            raise Exception('Number of column _indexes and column names must be equal.\n')

        if nulls is None:
            nulls = [(t != DataTypes.ROWTIME_INDICATOR and t != DataTypes.PROCTIME_INDICATOR)
                     for t in types]

        if len(names) != len(nulls):
            raise Exception('Number of column names and nullabilities must be equal.\n')

        columns = []
        for i in xrange(len(names)):
            columns.append(Column(names[i], types[i], nulls[i]))

        return columns

    def _java_to_python(self):
        """
        for internally using
        :return:
        """
        if self._j_table_schema is not None:
            j_arr = self._j_table_schema.getColumns()
            size = len(j_arr)
            self.columns = [Column(j_arr[i].name(),
                                   TypesUtil.to_py_sql_type(j_arr[i].internalType()),
                                   j_arr[i].isNullable())
                            for i in xrange(size)]
            j_arr = self._j_table_schema.getPrimaryKeys()
            size = len(j_arr)
            self.primary_keys = [j_arr[i] for i in xrange(size)]

            j_arr2 = self._j_table_schema.getUniqueKeys()
            self.unique_keys = [[j_arr2[i][j] for j in xrange(len(j_arr2[i]))] for i in xrange(len(j_arr2))]

            self.computed_columns = [ComputedColumn(jc.name(), jc.expression())
                                     for jc in self._j_table_schema.getComputedColumns()]

    class Builder(object):
        """
        >>> column = [Column('a', IntType()), Column('b', IntType()), Column('c', IntType()), Column('d', IntType())]
        >>> _primary_keys = ['a']
        >>> _unique_keys = [['a'], ['b', 'c']]
        >>> _indexes = [['a'], ['b', 'c']]
        >>> b = TableSchema.Builder(_columns=column, _primary_keys=_primary_keys, _unique_keys=_unique_keys, _indexes=_indexes)
        >>> schema = b.build()
        """

        _columns = []             # type: List[Column]
        _primary_keys = []        # type: List[str]
        _unique_keys = []         # type: List[List[str]]
        _indexes = []             # type: List[List[str]]
        _computed_columns = []    # type: List[ComputedColumn]
        _watermarks = []          # type: List[Watermark]

        def __init__(self, columns=[], primary_keys=[], unique_keys=[], indexes=[], computed_columns=[], watermarks=[]):

            self._columns = columns
            self._primary_keys = primary_keys
            self._unique_keys = unique_keys
            self._indexes = indexes
            self._computed_columns = computed_columns
            self._watermarks = watermarks

        def column(self, name, data_type):
            """
            Add a Column
            :param name:
            :param data_type:
            :return: Builder self
            """
            self._columns.append(Column(name=name, data_type=data_type))
            return self

        def primary_key(self, *fields):
            """
            Add primaryKey
            :param fields:
            :return:
            """
            if len(self._primary_keys) > 0:
                raise Exception(
                    'A _primary_keys key {0} have been defined, can not define another _primary_keys key'.format(
                        str(self.primary_key)))
            self._primary_keys.extend(fields)
            return self

        def unique_key(self, *fields):
            """
            Add an unique key
            :param fields:
            :return:
            """

            self._unique_keys.append(list(fields))  # [[]]
            self._indexes.append(list(fields))
            return self

        def computed_column(self, name, expression):
            self._computed_columns.append(ComputedColumn(name, expression))
            return self

        def watermark(self, name, event_time, offset):
            self._watermarks.append(Watermark(name, event_time, offset))
            return self

        def build(self):
            # build Python Object & Java Object
            j_builder_cls = TypesUtil.class_for_name('org.apache.flink.table.api.TableSchema.Builder')
            j_builder = j_builder_cls()

            for c in self._columns:
                j_builder.column(c.name, TypesUtil.to_java_sql_type(c.data_type), c.is_nullable)

            j_pks = TypesUtil._convert_py_list_to_java_array('java.lang.String', self._primary_keys)
            j_builder.primaryKey(j_pks)

            for uk in self._unique_keys:
                j_uks = TypesUtil._convert_py_list_to_java_array('java.lang.String', uk)
                j_builder.uniqueKey(j_uks)

            for cc in self._computed_columns:
                j_builder.computedColumn(cc.name, cc.expression)

            for wk in self._watermarks:
                j_builder.watermark(wk.name, wk.eventTime, wk.offset)

            j_schema = j_builder.build()
            return TableSchema(j_schema=j_schema)

        @staticmethod
        def _to_java(py_obj):
            if isinstance(py_obj, Column):
                j_column_cls = TypesUtil.class_for_name('org.apache.flink.table.api.Column')
                return j_column_cls(py_obj.name,
                                    TypesUtil.to_java_sql_type(py_obj.data_type),
                                    py_obj.is_nullable)
            elif isinstance(py_obj, ComputedColumn):
                j_column_cls = TypesUtil.class_for_name('org.apache.flink.table.api.ComputedColumn')
                return j_column_cls(py_obj.name, py_obj.expression)
            elif isinstance(py_obj, Watermark):
                j_watermark_cls = TypesUtil.class_for_name('org.apache.flink.table.api.WaterMark')
                return j_watermark_cls(py_obj.name, py_obj.eventTime, py_obj.offset)
