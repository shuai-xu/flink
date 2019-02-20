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

from py4j.java_gateway import get_method
from pyflink.table.table_schema import TableSchema


__all__ = [
    'Table',
    'WindowedTable',
    'WindowGroupedTable'
]


class Table(object):

    """
    Wrapper of org.apache.flink.table.api.Table
    """

    def __init__(self, java_table):
        self._java_table = java_table

    def select(self, col_list):
        return Table(self._java_table.select(col_list))

    def where(self, condition):
        return Table(self._java_table.select(condition))

    def group_by(self, key_list):
        return GroupedTable(self._java_table.groupBy(key_list))

    def as_(self, col_list):
        return Table(get_method(self._java_table, "as")(col_list))

    def create_temporal_table_function(
            self, time_attribute, primary_key):
        return Table(self._java_table.createTemporalTableFunction(
            time_attribute, primary_key))

    def distinct(self):
        return Table(self._java_table.distinct())

    def fetch(self, fetch_num):
        return Table(self._java_table.fetch(fetch_num))

    def filter(self, predicate):
        return Table(self._java_table.filter(predicate))

    def full_outer_join(self, right, join_predicate):
        return Table(self._java_table.fullOuterJoin(
            right._java_table, join_predicate))

    def insert_into(self, table_name):
        return Table(self._java_table.insertInto(table_name))

    def intersect(self, right):
        return Table(self._java_table.intersect(right._java_table))

    def intersect_all(self, right):
        return Table(self._java_table.intersectAll(right._java_table))

    def join(self, right):
        return Table(self._java_table.join(right._java_table))

    def join(self, right, join_predicate):
        return Table(self._java_table.join(right._java_table, join_predicate))

    def left_outer_join(self, right):
        return Table(self._java_table.leftOuterJoin(right._java_table))

    def left_outer_join(self, right, join_predicate):
        return Table(self._java_table.leftOuterJoin(
            right._java_table, join_predicate))

    def minus(self, right):
        return Table(self._java_table.minus(right._java_table))

    def minus_all(self, right):
        return Table(self._java_table.minusAll(right._java_table))

    def offset(self, offset):
        return Table(self._java_table.offset(offset))

    def order_by(self, fields):
        return Table(self._java_table.orderBy(fields))

    def print_schema(self):
        return Table(self._java_table.printSchema())

    def right_outer_join(self, right):
        return Table(self._java_table.rightOuterJoin(right._java_table))

    def right_outer_join(self, right, join_predicate):
        return Table(self._java_table.rightOuterJoin(
            right._java_table, join_predicate))

    def to_string(self):
        return Table(self._java_table.toString())

    def union(self, right):
        return Table(self._java_table.union(right._java_table))

    def union_all(self, right):
        return Table(self._java_table.unionAll(right._java_table))

    def window(self, window):
        return WindowedTable(self._java_table.window(window._java_window))

    def collect(self):
        return self._java_table.collect()

    def write_to_sink(self, j_sink):
        return self._java_table.writeToSink(j_sink._j_table_sink)

    def get_schema(self):
        # type: () -> TableSchema
        j_schema = self._java_table.getSchema()

        return TableSchema(j_schema=j_schema)


class GroupedTable(object):

    """
    Wrapper of org.apache.flink.table.api.GroupedTable
    """

    def __init__(self, java_table):
        self._java_table = java_table

    def select(self, col_list):
        return Table(self._java_table.select(col_list))


class WindowedTable(object):

    """
    Wrapper of org.apache.flink.table.api.WindowedTable
    """

    def __init__(self, java_table):
        self._java_table = java_table

    def group_by(self, fields):
        return WindowGroupedTable(self._java_table.groupBy(fields))


class WindowGroupedTable:

    """
    Wrapper of org.apache.flink.table.api.WindowGroupedTable
    """

    def __init__(self, java_table):
        self._java_table = java_table

    def select(self, col_list):
        return Table(self._java_table.select(col_list))





