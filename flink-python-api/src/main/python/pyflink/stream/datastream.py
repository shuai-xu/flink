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

    def __init__(self, j_ds):
        self._j_datastream = j_ds

    def _print(self):
        get_method(self._j_datastream, "print")()

    def add_sink(self, j_sink_func):
        # (JavaSinkFunction) -> DataStreamSink
        j_datastream_sink = self._j_datastream.addSink(j_sink_func._j_sink_function)
        return


class DataStreamSource(object):

    def __init__(self, ds_source):
        self._j_datastream_source = ds_source

    def set_parallelism(self, parallelism):
        # type: (int) -> None
        self._j_datastream_source.setParallelism(parallelism)


class DataStreamSink(object):

    def __init__(self, j_sink):
        self._j_datastream_sink = j_sink

    def name(self, name):
        self._j_datastream_sink.name(name)
        return self

    def set_uid_hash(self, uid_hash):
        self._j_datastream_sink.setUidHash(uid_hash)
        return self

    def set_parallelism(self, parallelism):
        # type: (int) -> DataStreamSink
        self._j_datastream_sink.setParallelism(parallelism)
        return self

    def set_config_item(self, key, value):
        # type: (str, str) -> DataStreamSink
        self._j_datastream_sink.setConfigItem(key, value)
        return self
