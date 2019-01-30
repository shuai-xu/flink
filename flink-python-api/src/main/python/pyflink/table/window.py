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

# This file is the wrappers for org.apache.flink.table.api.windows
import sys
import functools

from py4j.java_gateway import get_method
from pyflink.java_gateway import get_gateway

# TODO: expose Expression and support t.select(a.col, b.col2,..)

__all__ = [
    'Window',
    'Tumble',
    'TumbleWithSize',
    'TumbleWithSizeOnTime',
    'TumbleWithSizeOnTime',
    'TumbleWithSizeOnTimeWithAlias',
    'Session',
    'SessionWithGap',
    'SessionWithGapOnTime',
    'SessionWithGapOnTimeWithAlias' #,
    # 'Slide',
    # 'SlideWithSize',
    # 'SlideWithSizeAndSlide',
    # 'SlideWithSizeAndSlideOnTime',
    # 'SlideWithSizeAndSlideOnTimeWithAlias'
]


class Window(object):

    """
    Wrapper of org.apache.flink.table.api.Window
    """

    def __init__(self, java_window):
        self._java_window = java_window


@functools.total_ordering
class TimeWindow(Window):
    """

    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

        # for passing it to Java side
        self._j_time_window = get_gateway().jvm.org.apache.flink.table.api.window.TimeWindow(
            start,
            end
        )
        Window.__init__(self, self._j_time_window)

    def max_timestamp(self):
        return self.end - 1

    def intersects(self, other):
        return self.start <= other.end and self.end >= other.start

    def cover(self, other):
        return TimeWindow(min(self.start, other.start), max(self.end, other.end))

    # Python equivalent of Java's compareTo()
    def __lt__(self, other):
        if self.start == other.start:
            return self.end < other.end
        return self.start < other.start

    def __eq__(self, other):
        return self.start == other.start and self.end == other.end

    @classmethod
    def of(cls, start, to):
        return TimeWindow(start, to)

    @classmethod
    def get_window_start_with_offset(cls, timestamp, offset, window_size):
        return timestamp - (timestamp - offset + window_size) % window_size


@functools.total_ordering
class CountWindow(Window):
    def __init__(self, id):
        self.id = id

        # for passing it to Java side
        self._j_count_window = get_gateway().jvm.org.apache.flink.table.api.window.CountWindow(id)
        Window.__init__(self, self._j_count_window)

    def max_timestamp(self):
        return sys.maxint

    # Python equivalent of Java's compareTo()
    def __lt__(self, other):
        return self.id < other.id

    def __eq__(self, other):
        return self.id == other.id


class Tumble(object):

    """
    Wrapper of org.apache.flink.table.api.java.Tumble
    """
    @staticmethod
    def over(size):
        return TumbleWithSize(
            get_gateway().jvm.org.apache.flink.table.api.java.Tumble.over(size))


class TumbleWithSize(object):

    """
    Wrapper of org.apache.flink.table.api.TumbleWithSize
    """

    def __init__(self, java_window):
        self._java_window = java_window

    def on(self, time_field):
        return TumbleWithSizeOnTime(self._java_window.on(time_field))


class TumbleWithSizeOnTime(object):

    """
    Wrapper of org.apache.flink.table.api.TumbleWithSizeOnTime
    """

    def __init__(self, java_window):
        self._java_window = java_window

    def as_(self, alias):
        return TumbleWithSizeOnTimeWithAlias(
            get_method(self._java_window, "as")(alias))


class TumbleWithSizeOnTimeWithAlias(Window):

    """
    Wrapper of org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias
    """

    def __init__(self, java_window):
        Window.__init__(self, java_window)


class Session(object):

    @classmethod
    def with_gap(gap):
        return SessionWithGap(
            get_gateway().jvm.org.apache.flink.table.api.java.Session.withGap(gap))


class SessionWithGap(object):

    def __init__(self, java_window):
        self._java_window = java_window

    def on(self, time_field):
        return SessionWithGapOnTime(self._java_window.on(time_field))


class SessionWithGapOnTime(object):

    def __init__(self, java_window):
        self._java_window = java_window

    def as_(self, alias):
        return SessionWithGapOnTimeWithAlias(
            get_method(self._java_window, "as")(alias))


class SessionWithGapOnTimeWithAlias(Window):

    def __init__(self, java_window):
        Window.__init__(self, java_window)


# TODO: support Expression
# class Slide(object):
#
#     @classmethod
#     def over(cls, size):
#         return SlideWithSize(
#             get_gateway().jvm.org.apache.flink.table.api.java.Slide.over(size))


# class SlideWithSize(object):
#
#     def __init__(self, java_window):
#         self._java_window = java_window
#
#     def every(self, slide):
#         return SlideWithSizeAndSlide(self._java_window.every(slide))


# class SlideWithSizeAndSlide(object):
#
#     def __init__(self, java_window):
#         self._java_window = java_window
#
#     def on(self, time_field):
#         return SlideWithSizeAndSlideOnTime(self._java_window)


# class SlideWithSizeAndSlideOnTime(object):
#
#     def __init__(self, java_window):
#         self._java_window = java_window
#
#     def as_(self, alias):
#         return SlideWithSizeAndSlideOnTimeWithAlias(
#             get_method(self._java_window, "as")(alias))


# class SlideWithSizeAndSlideOnTimeWithAlias(Window):
#
#     def __init__(self, java_window):
#         Window.__init__(self, java_window)


class Over(object):

    @classmethod
    def order_by(cls, order_by):
        return OverWindowWithOrderBy(
            get_gateway().jvm.org.apache.flink.table.api.Over.orderBy(order_by))

    @classmethod
    def partition_by(cls, partition_by):
        return PartitionedOver(
            get_gateway().jvm.org.apache.flink.table.api.Over.partitionBy(
                partition_by))


class OverWindowWithOrderBy(object):

    def __init__(self, java_over_window):
        self._java_over_window = java_over_window

    def as_(self, alias):
        return OverWindow(get_method(self._java_over_window, "as")(alias))

    def preceding(self, preceding):
        return OverWindowWithPreceding(
            self._java_over_window.preceding(preceding))


class OverWindowWithPreceding(object):

    def __init__(self, java_over_window):
        self._java_over_window = java_over_window

    def as_(self, alias):
        return OverWindow(get_method(self._java_over_window, "as")(alias))

    def following(self, following):
        return OverWindowWithPreceding(
            self._java_over_window.following(following))


class PartitionedOver(object):

    def __init__(self, java_over_window):
        self._java_over_window = java_over_window

    def order_by(self, order_by):
        return OverWindowWithOrderBy(self._java_over_window.orderBy(order_by))


class OverWindow(object):

    def __init__(self, java_over_window):
        self._java_over_window = java_over_window
