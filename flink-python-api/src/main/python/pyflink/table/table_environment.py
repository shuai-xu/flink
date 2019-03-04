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
import sys

from pyflink import StreamExecutionEnvironment
from pyflink.stream import DataStream, DataStreamSource
from pyflink.java_gateway import get_gateway
from pyflink.table import Table
from pyflink.table.table_config import TableConfig
from pyflink.table.functions import *
from pyflink.util.type_util import TypesUtil
from pyflink.common.job_result import JobSubmissionResult, JobExecutionResult
from pyflink.stream.stream_graph import StreamGraph
from pyflink import cloudpickle

if sys.version > '3':
    xrange = range

__all__ = ['TableEnvironment']


class TableEnvironment(object):
    """
    Wrapper for org.apache.flink.table.api.java.TableEnvironment
    """
    table_config = None

    _DEFAULT_JOB_NAME = 'Flink Exec Table Job'

    def __init__(self, j_tenv):
        self._j_tenv = j_tenv

    def get_config(self):
        j_conf = self._j_tenv.getConfig()
        return TableConfig(j_conf)

    def get_catalog_manager(self):
        return self._j_tenv.getCatalogManager()

    def register_catalog(self, name, j_catalog):
        self._j_tenv.registerCatalog(name, j_catalog)

    def get_default_catalog_name(self):
        return self._j_tenv.getDefaultCatalogName()

    def set_default_catalog(self, name):
        self._j_tenv.setDefaultCatalog(name)

    def set_default_database(self, catalog_name, db_name):
        self._j_tenv.setDefaultDatabase(catalog_name, db_name)

    def set_default_database(self, *db_path):
        self._j_tenv.setDefaultDatabase(db_path)

    def get_table(self, name):
        self._j_tenv.getTable(name)

    def register_function(self, name, func):
        if isinstance(func, JavaScalarFunction) \
                or isinstance(func, JavaTableFunction)  \
                or isinstance(func, JavaAggregateFunction):
            self._j_tenv.registerFunction(name, func.j_func)
        # elif isinstance(func, Ta)
        else:
            clz = TypesUtil.class_for_name('org.apache.flink.api.python.PythonScalarUDF')
            pickled_bytes = bytearray(cloudpickle.dumps(func))
            j_type = TypesUtil.to_java_sql_type(func.eval.return_type)
            j_func = clz(name, pickled_bytes, j_type)
            self._j_tenv.registerFunction(name, j_func)

    def register_table(self, name, table):
        self._j_tenv.registerTable(name, table._java_table)

    def register_or_replace_table(self, name, table):
        self._j_tenv.registerOrReplaceTable(name, table._java_table)

    def register_table_source(self, name, table_source):
        self._j_tenv.registerTableSource(name, table_source.j_table_source)

    def register_or_replace_table_source(self, name, table_source):
        self._j_tenv.registerOrReplaceTableSource(name, table_source.j_table_source)


    # TODO def getTableStats
    # alterTableStats
    # alterSkewInfo
    # alterUniqueKeys

    def register_table_sink(self, name, field_names, field_types, table_sink):
        # j_field_names =
        j_names = TypesUtil._convert_py_list_to_java_array('java.lang.String',
                        field_names)
        j_types = []
        for field_type in field_types:
            j_types.append(TypesUtil.to_java_sql_type(field_type))
        j_types_array = TypesUtil._convert_py_list_to_java_array('org.apache.flink.table.types.DataType',
                            j_types)

        self._j_tenv.registerTableSink(name, j_names, j_types_array, table_sink._j_table_sink)

    def scan(self, *table_path):
        j_varargs = TypesUtil._convert_py_list_to_java_array('java.lang.String', table_path)
        j_table = self._j_tenv.scan(j_varargs)
        return Table(j_table)

    def list_catalogs(self):
        return self._j_tenv.listCatalogs()

    def list_databases(self):
        return self._j_tenv.listDatabases()

    def list_tables(self):
        return self._j_tenv.listTables()

    def list_user_defined_functions(self):
        return self._j_tenv.listUserDefinedFunctions()

    def explain(self, table):
        return self._j_tenv.explain(table._java_table)

    def sql_query(self, query):
        j_table = self._j_tenv.sqlQuery(query)
        return Table(j_table)

    # TODO: wrap QueryConfig
    def sql_update(self, stmt, config):
        if config is not None:
            j_table = self._j_tenv.sqlUpdate(stmt, config)
        else:
            j_table = self._j_tenv.sqlUpdate(stmt)

    def generate_stream_graph(self, job_name=None):
        if job_name is None:
            job_name = TableEnvironment._DEFAULT_JOB_NAME
        java_stream_graph = self._j_tenv.generateStreamGraph(job_name)
        return StreamGraph(java_stream_graph)

    def execute(self, job_name=None):
        if job_name is None:
            j_job_execution_result = self._j_tenv.execute()
        else:
            j_job_execution_result = self._j_tenv.execute(job_name)
        return JobExecutionResult(j_job_execution_result)

    # TODO: def setUserClassLoader

    @classmethod
    def get_table_environment(cls, stream_env, table_config=None):
        table_api = get_gateway().jvm.org.apache.flink.table.api
        j_env = stream_env._j_env
        if table_config is None:
            j_t_env = table_api.TableEnvironment.getTableEnvironment(j_env)
        else:
            j_t_env = table_api.TableEnvironment.getTableEnvironment(j_env, table_config._j_table_config)
        return StreamTableEnvironment(j_t_env)

    @classmethod
    def get_batch_table_environment(cls, stream_env, table_config=None):
        table_api = get_gateway().jvm.org.apache.flink.table.api
        j_env = stream_env._j_env
        if table_config is None:
            j_bt_env = table_api.TableEnvironment.getBatchTableEnvironment(j_env)
        else:
            j_bt_env = table_api.TableEnvironment.getBatchTableEnvironment(j_env, table_config._j_table_config)
        return BatchTableEnvironment(j_bt_env)

    @classmethod
    def validate_type(cls, t):
        j_type = TypesUtil._convert_to_java_type(t)
        table_api = get_gateway().jvm.org.apache.flink.table.api
        table_api.TableEnvironment.validateType(j_type)

    @classmethod
    def get_row_type_for_table_sink(cls, table_sink):
        table_api = get_gateway().jvm.org.apache.flink.table.api
        table_api.TableEnvironment.getRowTypeForTableSink(table_sink._j_table_sink)

    # TODO: getFieldNames/getFieldIndices/getFieldTypes


class StreamTableEnvironment(TableEnvironment):
    """
    Wrapper for org.apache.flink.table.api.java.StreamTableEnvironment
    """

    _j_tenv = None

    def __init__(self, env, table_config=None):
        self._j_tenv = env
        self.table_config = table_config

    def from_data_stream(self, data_stream, fields):
        # type: (DataStream, List) -> Table

        if fields is None:
            j_table = self._j_tenv.fromDataStream(data_stream._j_datastream_source)
        else:
            j_table = self._j_tenv.fromDataStream(data_stream._j_datastream_source, fields)

        return Table(j_table)

    def register_data_stream(self, name, data_stream, fields=None):
        if fields is None:
            self._j_tenv.registerDataStream(name, data_stream._j_datastream)
        else:
            self._j_tenv.registerDataStream(name, data_stream._j_datastream, fields)

    def register_or_replace_data_stream(self, name, data_stream, fields=None):
        if fields is None:
            self._j_tenv.registerOrReplaceDataStream(name, data_stream._j_datastream)
        else:
            self._j_tenv.registerOrReplaceDataStream(name, data_stream._j_datastream, fields)

    # TODO: clazz, StreamQueryConfig
    def to_append_stream(self, table, type_info):
        java_type_info = TypesUtil._convert_to_java_type(type_info)
        j_ds = self._j_tenv.toAppendStream(table._java_table, java_type_info)
        return DataStream(j_ds)

    # TODO: clazz, StreamQueryConfig
    def to_retract_stream(self, table, type_info):
        j_ds = self._j_tenv.toRetractStream(table._java_table, type_info)
        return DataStream(j_ds)


class BatchTableEnvironment(TableEnvironment):
    """
    Wrapper for org.apache.flink.table.api.java.BatchTableEnvironment
    """

    _j_tenv = None

    def __init__(self, env, table_config=None):
        self._j_tenv = env
        self.table_config = table_config

    def from_bounded_stream(self, bounded_stream, fields=None, field_nullables=None):
        j_table = self.j_env.fromBoundedStream(bounded_stream._j_datastream)
        return Table(j_table)

    def from_collection(self, data, fields=None):
        if type(data[0]) is tuple:
            java_list = TypesUtil._convert_tuple_list(data)
        else:
            java_list =TypesUtil._convert_pylist_to_java_list(data)
        if fields is None:
            j_table = self._j_tenv.fromCollection(java_list)
        else:
            j_table = self._j_tenv.fromCollection(java_list, fields)
        return Table(j_table)

    def register_bounded_stream(self, name, bounded_stream, fields=None):
        if fields is None:
            self._j_tenv.registerBoundedStream(name, bounded_stream._j_datastream)
        else:
            self._j_tenv.registerBoundedStream(name, bounded_stream._j_datastream, fields)

    def register_or_replace_bounded_stream(self, name, bounded_stream, fields=None):
        if fields is None:
            self._j_tenv.registerOrReplaceBoundedStream(name, bounded_stream._j_datastream)
        else:
            self._j_tenv.registerOrReplaceBoundedStream(name, bounded_stream._j_datastream, fields)

