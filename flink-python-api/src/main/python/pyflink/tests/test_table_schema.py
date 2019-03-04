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
from pyflink import StreamExecutionEnvironment
from pyflink.table import TableEnvironment
from pyflink.table.table_schema import TableSchema
from pyflink.sql.data_type import *
from pyflink.util.type_util import TypesUtil

import os
import pytest


@pytest.fixture(scope="module", autouse=True)
def init_pyflink_env():

    os.environ['FLINK_BIN_DIR'] = os.environ['FLINK_HOME'] + '/bin'
    print (os.environ['FLINK_BIN_DIR'])


def test_schema():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = TableEnvironment.get_table_environment(env)

    ds = env.from_collection(
        [(1L, "aa"),
         (2L, "bb"),
         (3L, "cc"),
         (4L, "dd"),
         (5L, "cc")])
    t = t_env.from_data_stream(ds, "id, name, c.proctime")
    schema1 = t.get_schema()
    print(str(schema1))

    '''
        root
        | -- name: id
        | -- type: LongType
        | -- isNullable: true
        | -- name: name
        | -- type: StringType
        | -- isNullable: true
        | -- name: c
        | -- type: ProctimeTimeIndicator
        | -- isNullable: false
    '''

    builder = TableSchema.Builder()
    builder.column('a', StringType())\
        .column('b', StringType()).column('c', StringType())
    builder.primary_key('a')
    builder.unique_key('a')
    schema2 = builder.build()
    print(str(schema2))
    '''
    root
     |-- name: a
     |-- type: StringType
     |-- isNullable: true
     |-- name: b
     |-- type: StringType
     |-- isNullable: true
     |-- name: c
     |-- type: StringType
     |-- isNullable: true
    primary keys
     |-- a
    unique keys
     |-- a
    indexes
     |-- a
     |-- a
    '''

    output_schema = TableSchema(names=['label_org', 'predict_label'], types=[IntegerType(), IntegerType()])


def test_rowtype():
    out_row_type = RowType([StringType(), IntegerType()], ["image_raw", "label"])
    j_row_type = TypesUtil.to_java_sql_type(out_row_type)
