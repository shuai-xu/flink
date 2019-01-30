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

"""
We serialize user's funcitons/operators, so that flink can support interactive mode.

CloudPickle is the default serializer, since it can handle closure, recursive functions.

"""
import sys
from pyflink import cloudpickle

if sys.version < '3':
    import cPickle as pickle
    protocol = 2
else:
    import pickle
    protocol = 3
    xrange = range


__all__ = ["UdfSerializer"]


class UdfSerializer(object):
    def dumps(self, obj):
        return cloudpickle.dumps(obj, protocol)

    if sys.version >= '3':
        def loads(self, obj, encoding="bytes"):
            return pickle.loads(obj, encoding=encoding)
    else:
        def loads(self, obj, encoding=None):
            return pickle.loads(obj)
