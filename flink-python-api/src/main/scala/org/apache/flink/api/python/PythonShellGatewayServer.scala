/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.python

import java.io.DataOutputStream
import java.net.Socket

import py4j.GatewayServer

/**
  * Python Shell Driver
  */
private[flink] object PythonShellGatewayServer {
  /**
    * <p>
    * Main method to start a local GatewayServer on a ephemeral port.
    * It tell python side via a handshake socket
    *
    * See: py4j.GatewayServer.main()
    * </p>
    */
  def main(args: Array[String]): Unit = {
    println("Starting PythonShellGatewayServer for flink python shell...")
    if (args.length == 0) {
      System.exit(1)
    }

    val gatewayServer = new GatewayServer(null, 0)
    gatewayServer.start()

    val serverPort = gatewayServer.getListeningPort

    // Tell python side the port of our java rpc server
    val handshakePort = args(0).toInt
    val localhost = "127.0.0.1"
    val handshakeSocket = new Socket(localhost, handshakePort)
    val dos = new DataOutputStream(handshakeSocket.getOutputStream)
    dos.writeInt(serverPort)
    dos.flush()
    dos.close()
    handshakeSocket.close()

    // Exit on EOF or broken pipe.  This ensures that the server dies
    // if its parent program dies.
    while (System.in.read() != -1) {
      // Do nothing
    }

    println("Existing...")
  }
}
