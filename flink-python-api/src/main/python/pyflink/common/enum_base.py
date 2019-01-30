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
from enum import Enum
from pyflink.java_gateway import get_gateway


# Flink's base enum class. For internal use only
# TODO:  if enum is not supported, we can implement by ourselves
class EnumBase(Enum):
    def convert_to_java_obj(self):
        return _EnumMap.enum_map().get(str(self))


class _EnumMap(object):
    """
    Helper class.
    """
    _map = None

    @classmethod
    def enum_map(cls):
        if _EnumMap._map is None:
            gateway = get_gateway()
            flink = gateway.jvm.org.apache.flink

            _EnumMap._map = {

                'TimeCharacteristic.ProcessingTime': flink.streaming.api.TimeCharacteristic.ProcessingTime,
                'TimeCharacteristic.IngestionTime': flink.streaming.api.TimeCharacteristic.IngestionTime,
                'TimeCharacteristic.EventTime': flink.streaming.api.TimeCharacteristic.EventTime,

                # org.apache.flink.streaming.api.functions.source.FileProcessingMode
                'FileProcessingMode.PROCESS_ONCE': flink.streaming.api.FileProcessingMode.PROCESS_ONCE,
                'FileProcessingMode.PROCESS_CONTINUOUSLY': flink.streaming.api.FileProcessingMode.PROCESS_CONTINUOUSLY
            }
        return _EnumMap._map
