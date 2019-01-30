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
from pyflink.util.type_util import TypesUtil


class JavaTableSink(object):
    """
    A wrapper for existing Java TableSink
    """
    def __init__(self, j_sink):
        self._j_table_sink = j_sink

    def get_output_type(self):
        j_data_type = self._j_table_sink.getOutputType()
        return TypesUtil.to_py_sql_type(j_data_type)

    def get_field_names(self):
        return self._j_table_sink.getFieldNames()

    def get_field_types(self):
        # Array
        j_data_types = self._j_table_sink.getFieldTypes()
        size = len(j_data_types)
        return [TypesUtil.to_py_sql_type(j_data_types[i]) for i in xrange(size)]

    def configure(self, field_names, field_types):
        size = len(field_types)
        j_field_types = [TypesUtil.to_java_sql_type(field_types[i]) for i in xrange(size)]
        j_names = TypesUtil._convert_py_list_to_java_array(
            'java.lang.String',
            field_names)
        j_types = TypesUtil._convert_py_list_to_java_array(
            'org.apache.flink.table.api.types.DataType',
            j_field_types)
        self._j_table_sink.configure(j_names, j_types)
        return self


