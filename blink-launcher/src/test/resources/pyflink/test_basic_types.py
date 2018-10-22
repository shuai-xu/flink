################################################################################
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
from flink.sql.udf import *
from flink.sql.types import *


class integer_udf(object):
    @udf(data_type=IntegerType())
    def eval(self, a, b):
        return a + b

    @udf(data_type=IntegerType())
    def eval(self, a):
        if a < 0:
            a = -100
        return a + 1


class varchar_udf(object):
    @udf(data_type=StringType())
    def eval(self, o):
        l = len(o)
        if l > 5:
            l = 5
        return "test-" + o[0:l]


class upper_udf(object):
    @udf(data_type=StringType())
    def eval(self, o):
        return o.upper()


class boolean_udf(object):
    @udf(data_type=BooleanType())
    def eval(self, o):
        return not o


class smallint_udf(object):
    @udf(data_type=ShortType())
    def eval(self, o):
        return o + 1


class tinyint_udf(object):
    @udf(data_type=ByteType())
    def eval(self, o):
        return o + 1


class bigint_udf(object):
    @udf(data_type=LongType())
    def eval(self, o):
        if o < 0:
            o = -1       # python Long -> Int
        return o + 1


class bigint_udf2(object):
    @udf(data_type=LongType())
    def eval(self, o):
        if o < 0:
            o = -1      # python Long -> Int
        return o + 1


class float_udf(object):
    @udf(data_type=FloatType())
    def eval(self, o):
        if o < 0:
            o = 0.111111
        return o + 1


from decimal import *


class decimal_udf(object):
    @udf(data_type=DecimalType(precision=20, scale=8))
    def eval(self, o):
        if o < 0:
            o = Decimal('-200000.142857')
        return Decimal('100000.142857')


class double_udf(object):
    @udf(data_type=DoubleType())
    def eval(self, o):
        return o + 1


import datetime
import time


class date_udf(object):
    @udf(data_type=DateType())
    def eval(self, o):
        return o


class time_udf(object):
    @udf(data_type=TimeType())
    def eval(self, o):
        return o


class timestamp_udf(object):
    @udf(data_type=TimestampType())
    def eval(self, o):
        return o


class varbinary_udf(object):
    @udf(data_type=BinaryType())
    def eval(self, o):
        return b"testbinary.." + o
