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

__all__ = ['DataStream', 'DataStreamSource']


# TODO: More DataStream APIs
class DataStream(object):
    j_ds = None

    def __init__(self, j_ds):
        self.j_ds = j_ds

    def _print(self):
        get_method(self.j_ds, "print")()


class DataStreamSource(object):
    j_ds_source = None

    def __init__(self, ds_source):
        self.j_ds_source = ds_source
    # todo: other functions...
