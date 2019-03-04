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
Python worker proxy server for communicating with JVM.
It runs at runtime, handles python UDF requests

Side-stepping the Global Interpreter Lock by using subprocesses instead of threads
(socket can not be closed. it is a python bug)

"""

import os
import sys
import struct
import multiprocessing
import threading
import tempfile
import datetime
import time
import decimal
import traceback
import platform

from socket import socket, AF_INET, AF_UNIX, SOCK_STREAM, SOMAXCONN

from pyflink import cloudpickle
from pyflink.sql.data_type import *

CPU_NUM = multiprocessing.cpu_count()
python_version = sys.version_info[0]
protocol_version = 1

# name -> udf object
# thread-safe
_udf_cache = {}


# [raw_len][protocol_version][action][action_data]
#    4         1                1     raw_len - 4 bytes
#
#  OPEN action data: [scalar_function_code][args_data]
class ActionFlags(object):
    ACTION_OPEN = -1
    ACTION_SCALAR_EVAL = -2
    ACTION_CLOSE = -3

    ACTION_SUCCESS = -4
    ACTION_FAIL = -5

    ACTION_COLLECT = -6


class StructFormats(object):
    PACK_LEN = '!I'  # UINT
    HEADER = '!2I'  # TODO...
    LEN = '!H'      # max length of a argument. 2 ** 16 = 65536
    TYPE = 'b'      # indicate the type of arguments. See: TypePrimitives
    BOOLEAN = '?'
    BYTE = 'B'   # unsigned char. tiny int, 0~255
    SHORT = '!h'
    USHORT = '!H'
    INT = '!i'
    LONG = '!q'     # 8 bytes. Java Long is 64bits
    FLOAT = '!f'
    DOUBLE = '!d'
    BINARY = 's'
    DATE = '!i'
    TIME = '!i'
    TIMESTAMP = '!q'


class HeaderFormat(object):
    LEN = '!i'
    LEN_SIZE = 4

    VER_FMT = 'b'  # signed char
    VER_LEN = 1

    ACT_FMT = 'b'  # signed char
    ACT_LEN = 1

    NUM_LEN = 4   # unsigned short, number of data


class PrimitiveTypes(object):
    NONE = 0        # we have to handle SQL's NULL / Java's null arguments
    STRING = 1
    BOOLEAN = 2
    SHORT = 3
    BYTE = 4
    INT = 5         # 4 bytes (32bits)
    LONG = 6        # Java Long is 8 bytes (64bits)
    FLOAT = 7       # Java float:  single-precision 32-bit IEEE 754 floating point
    DOUBLE = 8      # Java double: double-precision 64-bit IEEE 754 floating point
    BINARY = 9
    DATE = 10
    TIME = 11       # int,  milliseconds
    TIMESTAMP = 12
    DECIMAL = 13    # string representation, using scientific notation if an exponent is needed


class TimeConsts(object):
    EPOCH_ORDINAL = 719163      # the ordinal of 1970-01-01
    MILLIS_PER_DAY = 86400000   # = 24 * 60 * 60 * 1000  milliseconds in a day
    MILLIS_PER_HOUR = 3600000
    MILLIS_PER_MINUTE = 60000
    MILLIS_PER_SECOND = 1000


def receive_all(sock, data_len):
    chunks = []
    bytes_recd = 0
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 2048))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)
    return b''.join(chunks)


def pack_string(s):
    if s is None:
        return b''
    if python_version >= 3:
        byte_arr = s.encode('utf-8')
    else:
        byte_arr = s
    fmt = StructFormats.SHORT + ('%ds' % len(byte_arr))
    return struct.pack(fmt, len(byte_arr), byte_arr)


def recv_udf_call_raw_data(sock):
    chunks = []
    bytes_recd = 0

    # [LEN 8 bytes][Data]
    # make sure we received the LEN block
    data_len = HeaderFormat.LEN_SIZE
    while bytes_recd < data_len:
        chunk = sock.recv(max(data_len - bytes_recd, 8192))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    data = b''.join(chunks)
    v = memoryview(data)
    data_len = struct.unpack(StructFormats.INT, v[0:HeaderFormat.LEN_SIZE])[0]

    # if any data left
    while bytes_recd < data_len:
        chunk = sock.recv(min(data_len - bytes_recd, 8192))
        if chunk == b'':
            raise RuntimeError("socket connection broken")
        chunks.append(chunk)
        bytes_recd = bytes_recd + len(chunk)

    return b''.join(chunks)


def verify_udf(udf, udf_name):
    if hasattr(udf, 'open') and not hasattr(udf.open, '__call__'):
        raise Exception("{0}.open is not callable".format(udf_name))
    if not (hasattr(udf, 'eval') and hasattr(udf.eval, '__call__')):
        raise Exception("{0}.eval is not callable".format(udf_name))


def is_table_udf(udf):
    clz = udf.__class__
    while clz.__name__ != 'object':
        clz = clz.__base__
        if clz.__name__ == 'TableFunction' and clz.__module__ == 'pyflink.table.functions':
            return True

    return False


# [ACTION_OPEN][name_len][func_name][func_code_pickled_bytes]
def do_open_action(conn, v, idx):
    length = struct.unpack(StructFormats.SHORT, v[idx:idx+2])[0]
    idx = idx + 2
    udf_name = struct.unpack('{0}s'.format(length), v[idx:idx + length])[0]
    idx = idx + length
    udf = cloudpickle.loads(v[idx:])
    verify_udf(udf, udf_name)
    _udf_cache[udf_name] = udf

    # if it is a TableFunction
    if is_table_udf(udf):
        udf.sock = conn
        udf.collect = do_table_func_collect  # override collect

    if hasattr(udf, 'open'):
        udf.open()

    # [ver][act_flag]
    # [byte][byte]
    response = struct.pack('!2b', protocol_version, ActionFlags.ACTION_SUCCESS)
    conn.sendall(response)


def do_eval_action(conn, v, idx):
    length = struct.unpack(StructFormats.SHORT, v[idx:idx + 2])[0]
    idx = idx + 2
    udf_name = struct.unpack('{0}s'.format(length), v[idx:idx + length])[0]
    idx = idx + length

    # should be already cached
    udf = _udf_cache[udf_name]

    # parse args
    args_num = struct.unpack(StructFormats.SHORT, v[idx:idx + 2])[0]
    idx += 2
    meta = struct.unpack('%d%s' % (args_num, StructFormats.TYPE), v[idx:idx + args_num])
    idx += args_num

    result = []
    try:
        for m in meta:
            if m == PrimitiveTypes.NONE:
                result.append(None)
            elif m == PrimitiveTypes.STRING:
                arg_len = struct.unpack(StructFormats.LEN, v[idx:idx + 2])[0]
                idx += 2
                arg = struct.unpack('%ds' % arg_len, v[idx:idx + arg_len])[0]
                result.append(arg)
                idx += arg_len
            elif m == PrimitiveTypes.BOOLEAN:
                result.append(struct.unpack(StructFormats.BOOLEAN, v[idx:idx + 1])[0])
                idx += 1
            elif m == PrimitiveTypes.SHORT:
                result.append(struct.unpack(StructFormats.SHORT, v[idx:idx + 2])[0])
                idx += 2
            elif m == PrimitiveTypes.BYTE:
                result.append(struct.unpack(StructFormats.BYTE, v[idx:idx + 1])[0])
                idx += 1
            elif m == PrimitiveTypes.INT:
                result.append(struct.unpack(StructFormats.INT, v[idx:idx + 4])[0])
                idx += 4
            elif m == PrimitiveTypes.LONG:
                result.append(struct.unpack(StructFormats.LONG, v[idx:idx + 8])[0])
                idx += 8
            elif m == PrimitiveTypes.FLOAT:
                result.append(struct.unpack(StructFormats.FLOAT, v[idx:idx + 4])[0])
                idx += 4
            elif m == PrimitiveTypes.DOUBLE:
                result.append(struct.unpack(StructFormats.DOUBLE, v[idx:idx + 8])[0])
                idx += 8
            elif m == PrimitiveTypes.BINARY:
                arg_len = struct.unpack(StructFormats.LEN, v[idx:idx + 2])[0]
                arg = struct.unpack('%d%s' % (arg_len, StructFormats.BINARY), v[idx + 2:idx + 2 + arg_len])[0]
                result.append(arg)
                idx += (2 + arg_len)
            elif m == PrimitiveTypes.DATE:
                days = struct.unpack(StructFormats.DATE, v[idx:idx + 4])[0]
                d = datetime.date.fromordinal(days + TimeConsts.EPOCH_ORDINAL)
                result.append(d)
                idx += 4
            elif m == PrimitiveTypes.TIME:
                mills = struct.unpack(StructFormats.TIME, v[idx:idx + 4])[0]
                h = (mills % TimeConsts.MILLIS_PER_DAY) // TimeConsts.MILLIS_PER_HOUR
                m = (mills % TimeConsts.MILLIS_PER_HOUR) // TimeConsts.MILLIS_PER_MINUTE
                s = (mills % TimeConsts.MILLIS_PER_MINUTE) // TimeConsts.MILLIS_PER_SECOND
                mill = mills % TimeConsts.MILLIS_PER_SECOND
                result.append(datetime.time(h, m, s, mill))
                idx += 4
            elif m == PrimitiveTypes.TIMESTAMP:
                epoch = struct.unpack(StructFormats.TIMESTAMP, v[idx:idx + 8])[0]
                result.append(datetime.datetime.fromtimestamp(epoch / 1000.))
                idx += 8
            elif m == PrimitiveTypes.DECIMAL:
                arg_len = struct.unpack(StructFormats.LEN, v[idx:idx + 2])[0]
                dec_repr = struct.unpack('%ds' % arg_len, v[idx + 2:idx + 2 + arg_len])[0]
                result.append(decimal.Decimal(bytes.decode(dec_repr)))
                idx += 2 + arg_len
    except Exception as err:
        # todo
        raise err
        # error = build_error_response(
        #     'Exception is thrown when parsing arguments', err)
    args = tuple(result)

    # HEADER [ver][act_flag]
    # [num][types][data] # SHORT, bytes, ...
    res = udf.eval(*args)
    if is_table_udf(udf):
        response = serialize_py_result(ActionFlags.ACTION_SUCCESS, None, None)
    else:
        response = serialize_py_result(ActionFlags.ACTION_SUCCESS, (res,), (udf.eval.return_type,))

    conn.sendall(response)


def set_null_bit(bits, i):
    """
    :param bits: bitset
    :param i: colunm idx
    :return:
    """
    idx = (i // 8)
    bits[idx] |= (128 >> (i % 8))


def safe_buf_capacity_check(buf, capacity, idx, data_len):
    new_capacity = capacity
    while idx + data_len >= new_capacity:
        new_capacity = capacity * 2

    if new_capacity > capacity:
        new_buf = bytearray(new_capacity)
        new_buf[0:capacity] = buf
        return new_buf, new_capacity
    else:
        return buf, capacity


def serialize_field(buf, offset, sql_type, res):
    """

    :param buf:
    :param offset:
    :param sql_type:
    :param res:
    :return:
    """
    # String length limitation = 2 ^ 16 = 65536
    # BinaryType length limitation = 2 ^ 32

    # TODO: to be consistent with Flink's InternalRow, to bypass ser/des !
    if isinstance(sql_type, StringType):
        if python_version >= 3:
            byte_arr = res.encode('utf-8')
        else:
            byte_arr = res
        byte_num = len(byte_arr)
        # safe_buf_capacity_check()  # TODO

        struct.pack_into('!H%ds' % byte_num, buf, offset, byte_num, byte_arr)
        offset += (2 + byte_num)

    elif isinstance(sql_type, BinaryType):
        byte_num = len(res)
        struct.pack_into('!H%ds' % byte_num, buf, offset, byte_num, res)
        offset += (2 + byte_num)
    #
    # elif isinstance(sql_type, NullType):  # TODO: to be removed
    #     pass
    elif isinstance(sql_type, DateType):
        struct.pack_into('!i', buf, offset, res.toordinal() - TimeConsts.EPOCH_ORDINAL)
        offset += 4
    elif isinstance(sql_type, TimeType):
        ts = res.microsecond \
             + res.hour * TimeConsts.MILLIS_PER_HOUR \
             + res.minute * TimeConsts.MILLIS_PER_MINUTE \
             + res.second * TimeConsts.MILLIS_PER_SECOND
        struct.pack_into('!i', buf, offset, ts)
        offset += 4
    elif isinstance(sql_type, TimestampType):
        # no datetime.timestamp in python 2
        if python_version >= 3:
            ts = int(res.timestamp() * 1000)
        else:
            mills = res.microsecond // 1000
            ts = int(time.mktime(res.timetuple()) * 1000 + mills)
        struct.pack_into('!q', buf, offset, ts)
        offset += 8
    elif isinstance(sql_type, IntegerType):
        struct.pack_into('!i', buf, offset, res)
        offset += 4
    elif isinstance(sql_type, LongType):
        struct.pack_into('!q', buf, offset, res)
        offset += 8
    elif isinstance(sql_type, ByteType):
        struct.pack_into('!q', buf, offset, res)
        offset += 1
    elif isinstance(sql_type, DecimalType):
        s = str(res)
        byte_arr = s.encode('ascii')
        byte_num = len(byte_arr)
        struct.pack_into('!H%ds' % byte_num, buf, offset, byte_num, byte_arr)
        offset += 1
    elif isinstance(sql_type, BooleanType):
        struct.pack_into('?', buf, offset, res)
        offset += 1
    elif isinstance(sql_type, ShortType):
        struct.pack_into('!h', buf, offset, res)
        offset += 2
    elif isinstance(sql_type, FloatType):
        struct.pack_into('!f', buf, offset, res)
        offset += 4
    elif isinstance(sql_type, DoubleType):
        struct.pack_into('!d', buf, offset, res)
        offset += 8

    return buf, offset


def serialize_py_result(act_flag, data, return_types):
    """
    Format:  [version] [act_flag]  [arity]  [null bits][data]
              byte      byte        short
    :param act_flag:
    :param data:
    :param return_types:
    :return:
    """
    #
    buf = bytearray(4096)
    v = memoryview(buf)
    idx = 0
    struct.pack_into('2b', buf, idx, protocol_version, act_flag)

    # for TableFunction, eval() is avoid
    if data is None:
        return v[0:4]

    struct.pack_into('!H', buf, idx + 2, len(data))  # unsigned short
    idx = idx + (2 + 2)

    null_bits_len = (len(data) + 7) // 8  # bytes for null
    null_bits = bytearray(null_bits_len)
    idx += null_bits_len

    for i in xrange(len(data)):
        if data[i] is None:
            set_null_bit(null_bits, i)
        else:
            buf, idx = serialize_field(buf, idx, return_types[i], data[i])

    buf[4:4+null_bits_len] = null_bits

    return v[0:idx]


def do_table_func_collect(self, *args):
    """
    System behavior for TableFunction.collect()
    :param self:
    :return:
    """
    assert self.sock is not None
    response = serialize_py_result(ActionFlags.ACTION_COLLECT, args, self.eval.row_types)
    self.sock.sendall(response)


def request_dispatcher(conn):
    try:
        # loop for stream mode
        while True:
            data = recv_udf_call_raw_data(conn)
            try:
                if not data:
                    break
                v = memoryview(data)
                idx = HeaderFormat.LEN_SIZE + HeaderFormat.VER_LEN
                act = struct.unpack(HeaderFormat.ACT_FMT,
                                    v[idx:idx + HeaderFormat.ACT_LEN])[0]
                idx = idx + HeaderFormat.ACT_LEN
                if act == ActionFlags.ACTION_OPEN:
                    do_open_action(conn, v, idx)
                if act == ActionFlags.ACTION_SCALAR_EVAL:
                    do_eval_action(conn, v, idx)
            except Exception as e:
                # tell java the error in python side via socket
                err_msg = traceback.format_exc(limit=5)
                header = struct.pack('!2b', protocol_version, ActionFlags.ACTION_FAIL)
                conn.sendall(header + pack_string(err_msg))
    except Exception as e:
        # if socket is broken, print it to stderr
        traceback.print_exc(limit=5)
    finally:
        conn.close()


def shake_hands_with_jvm():
    try:
        jvm_port = int(sys.argv[1])

        server_sock, uds_name, tcp_port = create_socket_server()

        # Tell jvm the port or UDS namespace of python server
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(("127.0.0.1", jvm_port))

        packed_pathname = pack_string(uds_name)
        path_len = len(packed_pathname)
        pack_len = 8 + len(packed_pathname)
        # [len] [port] [pathname]
        # int32  int32    utf-8
        stream = struct.pack("!2i{0}s".format(path_len), pack_len, tcp_port, packed_pathname)
        sock.sendall(stream)
        sock.close()
        return server_sock
    except Exception as err:
        sys.stderr.write("[Python Error]: Can't raise python process correctly! " + str(err))
        server_sock.close()


def run_worker_server(server_sock):
    # running in server mode now_test_open
    try:
        # loop for multi-threads request
        while True:
            conn, addr = server_sock.accept()
            t = threading.Thread(target=request_dispatcher, args=(conn,))
            t.start()

    except socket.error as err:
        sys.stderr.write(str(err)+"\n")
    finally:
        server_sock.close()


def create_socket_server():
    pathname = None   # Unix Domain Socket binds to this namespace
    tcp_port = -1

    listen_num = max(CPU_NUM * 64, SOMAXCONN)
    sys_str = platform.system()
    if sys_str == "Linux" or sys_str == 'Darwin':    # Linux, Unix, Darwin support Unix Domain Socket
        pathname = tempfile.mktemp()

        # in case that the default temp folder path is too long
        # see:http://man7.org/linux/man-pages/man7/unix.7.html
        if len(pathname) > 92:
            pathname = tempfile.mktemp(dir='/tmp')
        try:
            os.unlink(pathname)
        except OSError:
            if os.path.exists(pathname):
                raise Exception("Failed to create a temp path for Unix Domain Socket")

        # create a named UDS
        server_sock = socket(AF_UNIX, SOCK_STREAM)
        server_sock.bind(pathname)
        server_sock.listen(listen_num)
    else:  # if sys_str == "Windows":
        # Right now, UDS is not mature enough on Windows?
        # Windows 10 17063 starts to support Unix Domain Socket
        # https://blogs.msdn.microsoft.com/commandline/2017/12/19/af_unix-comes-to-windows/
        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.bind(('127.0.0.1', 0))
        server_sock.listen(listen_num)
        host, tcp_port = server_sock.getsockname()

    return server_sock, pathname, tcp_port


def main():
    try:
        server_sock = shake_hands_with_jvm()
        run_worker_server(server_sock)
    except Exception as err:
        sys.stderr.write("[Python Error]: Can't raise python process correctly! " + str(err))


if __name__ == '__main__':
    main()
