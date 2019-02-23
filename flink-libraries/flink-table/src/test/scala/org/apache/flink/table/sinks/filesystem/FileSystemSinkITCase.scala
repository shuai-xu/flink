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

package org.apache.flink.table.sinks.filesystem

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes, TypeConverters}
import org.apache.flink.table.api.{RichTableSchema, TableConfig, TableConfigOptions}
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator
import org.apache.flink.table.factories.TableFactoryService
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.binaryRow
import org.apache.flink.table.sinks.{BatchTableSink, TableSink}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.TableProperties
import org.apache.flink.test.util.TestBaseUtils
import org.junit.rules.TemporaryFolder
import org.junit.{After, Before, Rule, Test}

import scala.collection.Seq
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import FileSystemSinkITCase.globalResults

class FileSystemSinkITCase extends BatchTestBase {
  val dataType = new BaseRowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)
  val fieldNames = Array("a", "b", "c")

  val data = Seq(
    binaryRow(dataType, 1, 1L, fromString("Hi")),
    binaryRow(dataType, 1, 1L, fromString("Hi01")),
    binaryRow(dataType, 1, 1L, fromString("Hi02")),
    binaryRow(dataType, 1, 1L, fromString("Hi03")),
    binaryRow(dataType, 1, 1L, fromString("Hi04")),
    binaryRow(dataType, 1, 2L, fromString("Hello")),
    binaryRow(dataType, 1, 2L, fromString("Hello01")),
    binaryRow(dataType, 1, 2L, fromString("Hello02")),
    binaryRow(dataType, 1, 2L, fromString("Hello03")),
    binaryRow(dataType, 1, 2L, fromString("Hello04")),
    binaryRow(dataType, 2, 1L, fromString("Hello world")),
    binaryRow(dataType, 2, 1L, fromString("Hello world01")),
    binaryRow(dataType, 2, 1L, fromString("Hello world02")),
    binaryRow(dataType, 2, 1L, fromString("Hello world03")),
    binaryRow(dataType, 2, 1L, fromString("Hello world04")),
    binaryRow(dataType, 2, 2L, fromString("Hello world, how are you?")),
    binaryRow(dataType, 3, 1L, fromString("I'm")),
    binaryRow(dataType, 3, 2L, fromString("I'm fine")),
    binaryRow(dataType, 3, 3L, fromString("I'm fine, thank")),
    binaryRow(dataType, 4, 1L, fromString("I'm fine, thank you")),
    binaryRow(dataType, 4, 2L, fromString("你好")),
    binaryRow(dataType, 4, 3L, fromString("你好，陌生")),
    binaryRow(dataType, 4, 4L, fromString("你好，陌生人")),
    binaryRow(dataType, 5, 1L, fromString("你好，陌生人，我是")),
    binaryRow(dataType, 5, 2L, fromString("你好，陌生人，我是中国人")),
    binaryRow(dataType, 5, 3L, fromString("你好，陌生人，我是中国人，你来自")),
    binaryRow(dataType, 5, 4L, fromString("你好，陌生人，我是中国人，你来自哪里？"))
  )

  val data1 = Seq(
    binaryRow(dataType, 2, 2L, fromString("Hi")),
    binaryRow(dataType, 1, 1L, fromString("Hello world")),
    binaryRow(dataType, 2, 2L, fromString("Hello")),
    binaryRow(dataType, 1, 1L, fromString("Hello world, how are you?")),
    binaryRow(dataType, 3, 3L, fromString("I'm fine, thank")),
    binaryRow(dataType, 3, 3L, fromString("I'm fine, thank you")),
    binaryRow(dataType, 3, 3L, fromString("I'm fine, thank you, and you?")),
    binaryRow(dataType, 4, 4L, fromString("你好，陌生人")),
    binaryRow(dataType, 4, 4L, fromString("你好，陌生人，我是")),
    binaryRow(dataType, 4, 4L, fromString("你好，陌生人，我是中国人")),
    binaryRow(dataType, 4, 4L, fromString("你好，陌生人，我是中国人，你来自哪里？"))
  )

  private[this] val formatFactory: FileSystemTableSinkFactory =
    TableFactoryService.find(classOf[FileSystemTableSinkFactory],
    Map(ConnectorDescriptorValidator.CONNECTOR_TYPE -> "FILESYSTEM"))

  private[this] def getCsvProperties(
    partitionColumns: Array[String] = Array("a", "b")): java.util.Map[String, String] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(Map(
      FileSystemOptions.PATH.key()-> resultPath,
      FileSystemOptions.FORMAT.key() -> "csv"
    ))
    tableProperties.putTableNameIntoProperties("csvFormatTable")
    val schema: RichTableSchema = new RichTableSchema(fieldNames,
      dataType.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
    if (partitionColumns.nonEmpty) {
      schema.setPartitionColumns(partitionColumns)
    }
    tableProperties.putSchemaIntoProperties(schema)
    tableProperties.toKeyLowerCase.toMap
  }

  val _tmpFolder = new TemporaryFolder()
  @Rule def tempFolder: TemporaryFolder = _tmpFolder
  private var resultPath: String = _
  private var fs: FileSystem = _

  private[this] def tableSink(
    props: Map[String, String] = null,
    partitionColumns: Array[String] = Array("a", "b")) = {
    val sinkProps = getCsvProperties(partitionColumns)
    if (props != null) {
      sinkProps.putAll(props)
    }
    formatFactory.createBatchTableSink(sinkProps)
  }

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    tEnv.registerCollection("nonSortTable", data, dataType, 'a, 'b, 'c)
    tEnv.registerCollection(tableName = "sortTable", data1, dataType, 'a, 'b, 'c)
    resultPath = tempFolder.newFolder().toURI.toString
    fs = new Path(resultPath).getFileSystem
  }

  @After
  def after(): Unit = {
    FileSystemSinkITCase.globalResults.clear()
  }

  @Test
  def testPartitionedAppendCSVSink(): Unit = {
    tEnv.sqlQuery("select a, b, c from nonSortTable").writeToSink(tableSink())
    tEnv.execute()
    validateFiles() { path =>
      getPathContent(path)
    }
  }

  @Test
  def testNonPartitionedAppendCSVSink(): Unit = {
    tEnv.sqlQuery("select a, b, c from nonSortTable")
      .writeToSink(tableSink(partitionColumns = Array()))
    tEnv.execute()
    validateFiles() { _ =>
      "1,1,Hi\n" +
        "1,1,Hi01\n" +
        "1,1,Hi02\n" +
        "1,1,Hi03\n" +
        "1,1,Hi04\n" +
        "1,2,Hello\n" +
        "1,2,Hello01\n" +
        "1,2,Hello02\n" +
        "1,2,Hello03\n" +
        "1,2,Hello04\n" +
        "2,1,Hello world\n" +
        "2,1,Hello world01\n" +
        "2,1,Hello world02\n" +
        "2,1,Hello world03\n" +
        "2,1,Hello world04\n" +
        "2,2,Hello world, how are you?\n" +
        "3,1,I'm\n" +
        "3,2,I'm fine\n" +
        "3,3,I'm fine, thank\n" +
        "4,1,I'm fine, thank you\n" +
        "4,2,你好\n" +
        "4,3,你好，陌生\n" +
        "4,4,你好，陌生人\n" +
        "5,1,你好，陌生人，我是\n" +
        "5,2,你好，陌生人，我是中国人\n" +
        "5,3,你好，陌生人，我是中国人，你来自\n" +
        "5,4,你好，陌生人，我是中国人，你来自哪里？\n"
    }
  }

  @Test
  def testSortLocalPartition(): Unit = {
    // configure the parallelism to be 1 so that we can sort globally.
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    val testSink = new TestTableSink(
      useLocalSort = true,
      partitionColumns = ArrayBuffer[String]("a", "b"),
      expectOutput = List(
        "1,1,Hello world",
        "1,1,Hello world, how are you?",
        "2,2,Hi",
        "2,2,Hello",
        "3,3,I'm fine, thank",
        "3,3,I'm fine, thank you",
        "3,3,I'm fine, thank you, and you?",
        "4,4,你好，陌生人",
        "4,4,你好，陌生人，我是",
        "4,4,你好，陌生人，我是中国人",
        "4,4,你好，陌生人，我是中国人，你来自哪里？"))
    tEnv.sqlQuery("select a, b, c from sortTable")
      .writeToSink(testSink)
    tEnv.execute()
    testSink.validate()
  }

  @Test
  def testNonSortLocalPartition(): Unit = {
    // configure the parallelism to be 1 so that this case result is stable.
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
    val testSink = new TestTableSink(
      useLocalSort = false,
      partitionColumns = ArrayBuffer[String]("a", "b"),
      expectOutput = List(
        "2,2,Hi",
        "1,1,Hello world",
        "2,2,Hello",
        "1,1,Hello world, how are you?",
        "3,3,I'm fine, thank",
        "3,3,I'm fine, thank you",
        "3,3,I'm fine, thank you, and you?",
        "4,4,你好，陌生人",
        "4,4,你好，陌生人，我是",
        "4,4,你好，陌生人，我是中国人",
        "4,4,你好，陌生人，我是中国人，你来自哪里？"))
    tEnv.sqlQuery("select a, b, c from sortTable")
      .writeToSink(testSink)
    tEnv.execute()
    testSink.validate()
  }

  @Test
  def testNonSortLocalPartitionMultipleTasks(): Unit = {
    val testSink = new TestTableSink(
      useLocalSort = false,
      partitionColumns = ArrayBuffer[String]("a", "b"),
      expectOutput = List(
        "1,1,Hello world",
        "1,1,Hello world, how are you?",
        "2,2,Hello",
        "2,2,Hi",
        "3,3,I'm fine, thank",
        "3,3,I'm fine, thank you",
        "3,3,I'm fine, thank you, and you?",
        "4,4,你好，陌生人",
        "4,4,你好，陌生人，我是",
        "4,4,你好，陌生人，我是中国人",
        "4,4,你好，陌生人，我是中国人，你来自哪里？"))
    tEnv.sqlQuery("select a, b, c from sortTable")
      .writeToSink(testSink)
    tEnv.execute()
    testSink.validate(true)
  }

  private[this] def validateFiles()(genContent: String => String): Unit = {
    //1. validate dirs are created correctly.
    //2. validate file content are also correct.
    val path = new Path(resultPath)
    comparePath(path)(genContent)
  }

  private def comparePath(p: Path)(genContent: String => String): Unit = {
    val locatedFiles = fs.listLocatedStatus(p)
    locatedFiles.foreach { lfs =>
      if (lfs.isDir) {
        comparePath(lfs.getPath) { p =>
          genContent(p)
        }
      } else {
        val ori = lfs.getPath
        TestBaseUtils.compareResultAsText(genContent(ori.toString).split("\n").toList,
          scala.io.Source.fromURI(ori.toUri).getLines().mkString("\n"))
      }
    }
  }

  class TestTableSink(
    val useLocalSort: Boolean,
    val partitionColumns: ArrayBuffer[String],
    val expectOutput: List[String])
      extends BatchTableSink[BaseRow]
      with DefinedDistribution
      with Serializable {

    override def emitBoundedStream(
      boundedStream: DataStream[BaseRow],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[_] = {
      boundedStream.addSink(new SinkFunction[BaseRow] {
        override def invoke(value: BaseRow, sinkFunction: SinkFunction.Context[_]): Unit = {
          val outputStr = s"${value.getInt(0)}," +
            s"${value.getLong(1)}," +
            s"${value.getString(2)}"
          FileSystemSinkITCase.synchronized(FileSystemSinkITCase.globalResults += outputStr)
        }
      })
    }

    override def getOutputType: DataType = dataType

    override def getFieldNames: Array[String] = fieldNames

    override def getFieldTypes: Array[DataType] =
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING)

    override def configure(fieldNames: Array[String], fieldTypes: Array[DataType])
    : TableSink[BaseRow] = {
      this
    }

    override def getPartitionFields(): Array[String] = partitionColumns.toArray

    override def sortLocalPartition(): Boolean = useLocalSort

    def validate(sortResult: Boolean = false): Unit = {
      assert(globalResults.size == expectOutput.size,
        s"The output only have ${globalResults.size} rows while expect to be ${expectOutput.size}.")
      val targetResults = if (sortResult) {
        globalResults.sorted
      } else {
        globalResults
      }
      targetResults zip expectOutput foreach {
        t => assert(t._1 == t._2, s"The output row is ${t._1} while expect to be ${t._2}.")
      }
    }
  }

  private[this] def getPathContent(path: String): String = {
    if (path.contains("a=1/b=1")) {
      "1,1,Hi\n" +
        "1,1,Hi01\n" +
        "1,1,Hi02\n" +
        "1,1,Hi03\n" +
        "1,1,Hi04\n"
    } else if (path.contains("a=1/b=2")) {
      "1,2,Hello\n" +
        "1,2,Hello01\n" +
        "1,2,Hello02\n" +
        "1,2,Hello03\n" +
        "1,2,Hello04\n"
    } else if (path.contains("a=2/b=1")) {
      "2,1,Hello world\n" +
        "2,1,Hello world01\n" +
        "2,1,Hello world02\n" +
        "2,1,Hello world03\n" +
        "2,1,Hello world04\n"
    } else if (path.contains("a=2/b=2")) {
      "2,2,Hello world, how are you?\n" // this contains quote character defaults to be "
    } else if (path.contains("a=3/b=1")) {
      "3,1,I'm"
    } else if (path.contains("a=3/b=2")) {
      "3,2,I'm fine"
    } else if (path.contains("a=3/b=3")) {
      "3,3,I'm fine, thank"
    } else if (path.contains("a=4/b=1")) {
      "4,1,I'm fine, thank you"
    } else if (path.contains("a=4/b=2")) {
      "4,2,你好"
    } else if (path.contains("a=4/b=3")) {
      "4,3,你好，陌生"
    } else if (path.contains("a=4/b=4")) {
      "4,4,你好，陌生人"
    } else if (path.contains("a=5/b=1")) {
      "5,1,你好，陌生人，我是"
    } else if (path.contains("a=5/b=2")) {
      "5,2,你好，陌生人，我是中国人"
    } else if (path.contains("a=5/b=3")) {
      "5,3,你好，陌生人，我是中国人，你来自"
    } else if (path.contains("a=5/b=4")) {
      "5,4,你好，陌生人，我是中国人，你来自哪里？"
    } else {
      throw new RuntimeException(s"Unexpected write path: $path")
    }
  }
}

object FileSystemSinkITCase {
  var globalResults: ArrayBuffer[String] = new ArrayBuffer[String]()
}
