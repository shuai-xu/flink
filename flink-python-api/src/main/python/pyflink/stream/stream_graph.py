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
from pyflink.stream.enums import TimeCharacteristic
from pyflink.util.type_util import TypesUtil
from pyflink.common.cache import DistributedCache

__all__ = ['StreamGraph']


class StreamGraph(object):
    """
    Wrapper for org.apache.flink.streaming.api.graph.StreamGraph
    """
    def __init__(self, j_stream_graph):
        self._j_stream_graph = j_stream_graph

    def get_execution_config(self):
        # py4j's 'java object'
        return self._j_stream_graph.getExecutionConfig()

    def get_checkpoint_config(self):
        return self._j_stream_graph.getCheckpointConfig()

    def set_job_name(self, job_name):
        self._j_stream_graph.setJobName(job_name)

    def set_job_name(self, job_name):
        self._j_stream_graph.setJobName(job_name)

    def set_chaining(self, chaining):
        self._j_stream_graph.setChaining(chaining)

    def set_multi_head_chain_mode(self, multi_head_chain_mode):
        self._j_stream_graph.setMultiHeadChainMode(multi_head_chain_mode)

    def set_time_characteristic(self, time_characteristic):
        self._j_stream_graph.setTimeCharacteristic(time_characteristic.convert_to_java_obj())

    def get_time_characteristic(self):
        java_obj = self._j_stream_graph.getTimeCharacteristic()
        return TypesUtil._convert_tuple_list(java_obj)

    def get_cached_files(self):
        j_files = self._j_stream_graph.getCachedFiles()
        py_list = []
        types = [unicode, DistributedCache.DistributedCacheEntry]
        for i in range(j_files):
            py_tuple = TypesUtil._java_tuple_to_tuple(j_files[i], types)
            py_list.append(py_tuple)
        return py_list

    def set_cached_files(self, cached_files):
        java_cached_files = TypesUtil._convert_pylist_to_java_list(cached_files)
        self._j_stream_graph.setCachedFiles(java_cached_files)

    def set_chain_eagerly_enabled(self, chain_eagerly_enabled):
        self._j_stream_graph.setChainEagerlyEnabled(chain_eagerly_enabled)

    def is_chaining_enabled(self):
        self._j_stream_graph.isChainingEnabled()

    def is_multi_head_chain_mode(self):
        self._j_stream_graph.isMultiHeadChainMode()

    def is_chain_eagerly_enabled(self):
        self._j_stream_graph.isChainEagerlyEnabled()

    def set_parallelism(self, vertex_id, parallelism):
        self._j_stream_graph.setParallelism(vertex_id, parallelism)

    def set_max_parallelism(self, vertex_id, max_parallelism):
        self._j_stream_graph.setMaxParallelism(vertex_id, max_parallelism)

    # TypeInformation < OUT >
    def set_out_type(self, vertex_id, out_type):
        self._j_stream_graph.setOutType(vertex_id, out_type)

    # TODO: other functions

    def get_source_ids(self):
        return self._j_stream_graph.getSourceIDs()

    def get_streaming_plan_as_json(self):
        return self._j_stream_graph.getStreamingPlanAsJSON()

    def dumpStreamingPlanAsJSON(self, file):
        file.write(self.get_streaming_plan_as_json())




