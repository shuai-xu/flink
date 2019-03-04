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

import org.apache.flink.core.fs.Path

import collection.JavaConverters._
import py4j.GatewayServer

/**
  * Python Driver.
  */
private[flink] object PythonDriver {

  /**
    *
    * @param args:   [--py-files package.zip,venv.zip] [main.py | --mod com.main] [arg1 arg2...]
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      System.exit(1)
    }

    // parser args
    val argsList = collection.mutable.Map[String, Any]()
    parseOption(args.toList, argsList)

    val gatewayServer = new py4j.GatewayServer(null, 0)
    val thread = new Thread(new Runnable() {
      override def run(): Unit =  {
        gatewayServer.start()
      }
    })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()
    thread.join()

    // prepare python env
    val fileMap = collection.mutable.Map[String, String]()
    argsList.getOrElse("--py-files", List[String]())
      .asInstanceOf[List[String]].foreach { filePath =>
      val path = new Path(filePath)
      fileMap(path.getName) = filePath
    }
    argsList.get("main_py").foreach { py =>
      val path = new Path(py.asInstanceOf[String])
      fileMap(path.getName) = py.asInstanceOf[String]
    }

    val commands = collection.mutable.MutableList[String]()
    if (argsList.get("--mod").nonEmpty && argsList.get("main_py").nonEmpty) {
      throw new RuntimeException("Both python module and main driver python file are specified!")
    }

    argsList.get("--mod").foreach( driverModule => {
      commands += "--mod"
      commands += driverModule.asInstanceOf[String]
    })

    argsList.get("main_py").foreach( mainPy => {
      // we will copy user's file to a local temp folder in case that
      // the user's python file is in remote server, such as HDFS
      val path = new Path(mainPy.asInstanceOf[String])
      commands += path.getName
    })

    argsList.getOrElse("args", List[String]()).asInstanceOf[List[String]].foreach(arg =>
      commands += arg
    )

    try {
      val pythonEnv = PythonUtil.preparePythonEnvironment(fileMap.asJava)
      pythonEnv.sysVariables.put("PYFLINK_GATEWAY_PORT", gatewayServer.getListeningPort.toString)
      val pythonProcess = PythonUtil.startPythonProcess(pythonEnv, commands.toArray, -1)

      val exitCode = pythonProcess.waitFor()
      if (exitCode != 0) {
        throw new RuntimeException("Python process exits with code: " + exitCode)
      }
    } finally {
      gatewayServer.shutdown()
    }
  }

  def parseOption(argsList: List[String], args: collection.mutable.Map[String, Any]): Unit = {

    argsList match {
      case Nil => return
      case "--py-files" :: value :: tail =>
        args("--py-files") = value.split(",").toList
        parseOption(tail, args)
      case "--mod" :: value :: tail =>
        args("--mod") = value
        parseOption(tail, args)
      case value :: tail =>
        if (value.endsWith(".py")) {
          args("main_py") = value
          parseOption(tail, args)
        }
        else {
          args("args") = args.getOrElse("args", List[String]()).asInstanceOf[List[String]].:+(value)
          parseOption(tail, args)
        }
    }
  }
}
