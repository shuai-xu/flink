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
from abc import ABCMeta, abstractmethod

__all__ = [
    'JavaScalarFunction',
    'JavaAggregateFunction',
    'JavaTableFunction',
    'ScalarFunction'
]


class JavaScalarFunction(object):
    """
    A wrapper of existing java scalar function
    """
    def __init__(self, j_scalar_func):
        self.j_func = j_scalar_func


class JavaAggregateFunction(object):
    """
     A wrapper of existing java AggregateFunction
    """
    def __init__(self, j_agg_func):
        self.j_func = j_agg_func


class JavaTableFunction(object):
    """
    A wrapper of existing java TableFunction
    """

    def __init__(self, j_table_func):
        self.j_func = j_table_func


class ScalarFunction(object):
    """
    An interface for writing python scalar function
    # TODO (WIP)
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def eval(self, *args):
        """Please define your implementation"""

    def open(self, *args):
        pass

    def close(self, *args):
        pass

    @classmethod
    def is_deterministic(cls):
        return False

    # TODO:  Using decorator to implement getParameterTypes(self):
    # public DataType getResultType(Object[] arguments, Class[] argTypes)

