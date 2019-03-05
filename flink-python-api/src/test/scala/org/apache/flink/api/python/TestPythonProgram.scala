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

import collection.JavaConverters._

import org.junit.{AfterClass, Before, BeforeClass, Test}

import java.io.File

class TestPythonProgram {

  @Test
  def testPythonFiles() = {

    val testFiles = Array("test_e2e.py")
    testFiles.foreach(f => {
      val commands = Array(f)
      val pythonProcess = PythonUtil.startPythonProcess(TestPythonProgram.pythonEnv, commands, -1)

      val exitCode = pythonProcess.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Python process exits with code: " + exitCode)
      }
    })
  }
}

object TestPythonProgram {

  var gatewayServer: py4j.GatewayServer = null
  var pythonEnv: PythonUtil.PythonEnvironment = null

  @BeforeClass
  def startGateway(): Unit = {
    gatewayServer = new py4j.GatewayServer(null, 0)
    val thread = new Thread(new Runnable() {
      override def run(): Unit =  {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()
    thread.join()

    val fileMap = collection.mutable.Map[String, String]()

    val testFiles = Array("test_e2e.py", "test_udf.py")
    testFiles.foreach( f => {
      fileMap(f) = TestPythonProgram.getResourcePath(f)
    })

    pythonEnv = PythonUtil.preparePythonEnvironment(fileMap.asJava)
    pythonEnv.sysVariables.put("PYFLINK_GATEWAY_PORT",
      TestPythonProgram.gatewayServer.getListeningPort.toString)
  }

  @AfterClass
  def stopGateway() = {
    if (gatewayServer != null) {
      gatewayServer.shutdown()
    }
  }

  def getResourcePath(relativePath: String): String = {
    return new File(classOf[TestPythonProgram]
      .getClassLoader
      .getResource(relativePath).getFile).getAbsolutePath
  }
}
