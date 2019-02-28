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
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.io.{CleanupWhenUnsuccessful, FinalizeOnMaster, InitializeOnMaster, OutputFormat, RichOutputFormat}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.InputTypeConfigurable
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.{CodeGeneratorContext, ProjectionCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.types.RowType
import org.apache.flink.table.util.{Logging, LowerCaseKeyMap}

import javax.annotation.Nullable

import java.io.IOException
import java.util.{TimeZone, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Abstract class for writing data in single Flink task.
  * Exceptions thrown by the implementation of this trait
  *
  * Notes:
  *
  * 1. This class must be serializable.
  * 2. We make this class subclass of OutputFormat only to make use of [[FinalizeOnMaster]]
  * and [[InitializeOnMaster]] interface, cause now they only work for [[OutputFormat]]
  * during runtime, this inheritance expects to be removed if Flink runtime
  * supports a pre/post single job execution apart from sql job.
  */
abstract class FileSystemOutputFormat(
    val path: String,
    val description: OutputJobDescription)
  extends OutputFormat[BaseRow]
  with Serializable
  with FinalizeOnMaster
  with InitializeOnMaster {

  // ------------- Members and methods about file handles management. -----------------------
  private val workDirName = ".flink-workdir-" + description.uuid

  private val workDir = new Path(path, workDirName)

  @transient private var fs: FileSystem = _

  // ------------- OutputFormat configuration. -----------------------
  private var conf: Configuration = _
  private var runtimeContext: RuntimeContext = _
  private var execConf: ExecutionConfig = _
  private var inputType: TypeInformation[_] = _

  /**
    * Checks whether there are files to be committed to a valid output location. This is only needed
    * if we write to distributed file systems.
    */
  private val pathIsValid = Try { new Path(path) }.isSuccess

  @transient private var tempWriteFiles: mutable.Set[String] = null

  /**
    * Added partition paths, used to release resource when abort this task.
    * It is not serializable, subclass should instantiate it when it is needed.
    */
  @transient protected var addedPartitions: mutable.Set[String] = _
  var currentFormat: OutputFormat[BaseRow] = _

  /**
    * Max number of files a single task writes out in order to limit file size. The files number
    * should not be too big in common case. This is just a guard for some bad cases.
    */
  protected val MAX_FILE_NUM: Int = 1000 * 1000

  /**
    * Get a new task temp file name to write data to. Must be called on TM tasks.
    *
    * A full file path consists of the following parts:
    *
    * | 1. basePath | 2. partition path(e.g. "a=1/b=2") | 3. uniqueId(uuid or jobID) |
    * 4. file extension(e.g. ".csv")
    *
    * Notes: It's important to know that the scope of this func is only in task lifetime,
    * for a single task scope, this task can guarantee that the file name is unique, but is not
    * true for whole job, so it's the user's responsibility to keep the file name unique globally.
    *
    * @param taskId index id of current task, start from 0.
    * @param dir: partition path.
    * @param ext: file extension.
    */
  protected def newTaskOutputFile(taskId: Int, dir: Option[String], ext: String): String = {
    val fileName = getFileName(taskId, ext)
    val path = if (dir.isDefined) {
      new Path(new Path(workDir, dir.get), fileName).toString
    } else {
      new Path(workDir, fileName).toString
    }
    tempWriteFiles += path
    path
  }

  /**
    * The file name pattern is part-taskId-uuid-c$fileCount.orc,
    * see [[newTaskOutputFile]] for details.
    */
  private def getFileName(taskId: Int, ext: String): String = {
    f"part-$taskId%05d-${description.uuid}$ext"
  }

  def setConfiguration(configuration: Configuration): Unit = {
    this.conf = configuration
  }

  def setRuntimeContext(runtimeContext: RuntimeContext) = {
    this.runtimeContext  = runtimeContext
  }

  def setExecutionConfig(executionConfig: ExecutionConfig): Unit = {
    this.execConf = executionConfig
  }

  def setInputType(inputType: TypeInformation[_]): Unit = {
    this.inputType = inputType
  }

  /**
    * Set up this OutputFormat when task starts, this is only invoked one time for the whole
    * write task life time.
    */
  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    addedPartitions = mutable.Set[String]()
    tempWriteFiles = mutable.Set[String]()
  }

  override def initializeGlobal(parallelism: Int): Unit = {
    fs = FileSystemOutputFormat.initializeFileSystem(workDir, null)
    if (fs.exists(workDir)) {
      throw new RuntimeException(s"Working dir ${workDir.toUri.toString} already exists.")
    }
    fs.mkdirs(workDir)
  }

  override def finalizeGlobal(parallelism: Int): Unit = {
    if (pathIsValid) {
      // 1.1 clear abs path.
      // 1.2 rename addedAbsPathFiles tmp dirs to final

      // 2.1 clear finalPath = path + partitionPaths
      // 2.2 rename workDir + part to finalPath
      // Notes:
      // 1. At this moment, we do not support absolute dirs yet, we will rename all files under
      //    workDir Dir to path Dir
      // 2. The move sequence is depth first
      fs = FileSystemOutputFormat.initializeFileSystem(workDir, null)
      movePath(workDir)
      // finally delete staging dir.
      fs.delete(workDir, true)
    }
  }

  private def movePath(p: Path): Unit = {
    val locatedFiles = fs.listLocatedStatus(p)
    locatedFiles.foreach { lfs =>
      if (lfs.isDir) {
        movePath(lfs.getPath)
      } else {
        val ori = lfs.getPath
        val dest = pathRemovedWithWorkDir(ori)
        fs.rename(ori, dest)
      }
    }
  }

  private def pathRemovedWithWorkDir(path: Path): Path = {
    val paths = path.toString.split(Path.SEPARATOR)
    val pathBuf: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]()
    for(p <- paths) {
      if(p != workDirName) {
        pathBuf += p
      }
    }
    new Path(pathBuf.mkString(Path.SEPARATOR))
  }

  protected def closeCurrentFormat(): Unit = {
    if (currentFormat != null) {
      currentFormat.close()
      currentFormat == null
    }
  }

  def abort(): Unit = {
    try {
      closeCurrentFormat()
    } finally {
      for (p <- tempWriteFiles) {
        fs.delete(new Path(p), false)
      }
    }
  }

  /** Get current task ID, we can do this only after [[runtimeContext]] has been set. **/
  def getTaskID: Int = {
    runtimeContext.getIndexOfThisSubtask
  }

  def getCurrentFormat: OutputFormat[BaseRow] = {
    currentFormat
  }

  /** Should be invoked every time we initialize a new [[currentFormat]] instance. */
  protected def configCurrentFormat(): Unit = {
    assert(currentFormat != null, "Can't config current format cause it's null!")
    currentFormat.configure(conf)
    currentFormat.open(runtimeContext.getIndexOfThisSubtask,
      runtimeContext.getNumberOfParallelSubtasks)
    currentFormat match {
      case rf: RichOutputFormat[_] =>
        rf.setRuntimeContext(runtimeContext)
      case ic: InputTypeConfigurable =>
        ic.setInputType(inputType, execConf)
    }
  }

  /** Close and flush current format. **/
  protected def releaseCurrentFormat(): Unit = {
    if (currentFormat != null) {
      releaseOutputFormat(currentFormat)
      currentFormat = null
    }
  }

  private[this] def releaseOutputFormat(format: OutputFormat[_]): Unit = {
    var cleanupCalled: Boolean = false
    try {
      format.close()
    } catch {
      case _: Exception =>
        format match {
          case unsuccessful: CleanupWhenUnsuccessful if !cleanupCalled =>
            unsuccessful.tryCleanupOnError()
            // may invoke clean up two times ?
            cleanupCalled = true
          case _ =>
        }
    }
  }

  /** New OutputFormat instance and use it as the current writer to output data.
    *
    * Notes:
    * 1. If the data is sorted by partition values, that means we will use only one OutputFormat
    * once at a time, so the previous OutputFormat instance can be released safely.
    * 2. If the data is not sorted, we cache the previous created OutputFormat instances and reuse
    * them depends on the current partition path.
    * */
  protected def refreshCurrentFormat(partitionKey: Option[BaseRow]): Unit = ???

  override def configure(parameters: Configuration): Unit = {
    // do nothing now.
  }

  override def close(): Unit = {
    closeCurrentFormat()
  }
}

/** A shared job description for all the write tasks. */
class OutputJobDescription(
  val uuid: String, // prevent collision between different (appending) write jobs
  val outputFormatFactory: OutputFormatFactory,
  val dataSchema: RowType,
  val partitionSchema: Option[RowType],
  val timeZone: TimeZone
) extends Serializable

/** Tool object for writing FileFormat data out to a filesystem. */
object FileSystemOutputFormat extends Logging {
  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(
    outputPath: String,
    customPartitionLocations: Map[CustomTablePartition, String],
    outputColumns: RowType)

  def getOutputJobDescription(
      outputFormatFactory: OutputFormatFactory,
      outputSpec: OutputSpec,
      partitionColumns: Option[RowType],
      options: java.util.Map[String, String]): OutputJobDescription = {

    val caseInsensitiveOptions = LowerCaseKeyMap(options.asScala.toMap)
    val timeZoneStr = caseInsensitiveOptions.getOrElse(FileSystemOptions.TIME_ZONE.key(),
      FileSystemOptions.TIME_ZONE.defaultValue())
    // We do not remove partition columns from data column now.
    val dataSchema = outputSpec.outputColumns

    outputFormatFactory.configure(options)
    new OutputJobDescription(
      uuid = UUID.randomUUID().toString,
      outputFormatFactory = outputFormatFactory,
      dataSchema = dataSchema,
      partitionSchema = partitionColumns,
      timeZone = TimeZone.getTimeZone(timeZoneStr))
  }

  /** Writes data out in a single Flink task. */
  def getFileFormatOutput(
      description: OutputJobDescription,
      jobID: String,
      path: String): FileSystemOutputFormat = {

    val dataOutput =
      if (description.partitionSchema.nonEmpty
        && description.partitionSchema.get.getFieldNames.nonEmpty) {
        val dataSchema = description.dataSchema
        val partitionSchema = description.partitionSchema.get

        val generatedProjectValues = ProjectionCodeGenerator.generateProjection(
          CodeGeneratorContext(new TableConfig),
          "PartitionValuesProjection",
          dataSchema,
          partitionSchema,
          partitionSchema.getFieldNames.map(dataSchema.getFieldNames.indexOf(_)))
        new DynamicPartitionOutput(path, description, generatedProjectValues)
      } else {
        new SingleDirectoryOutput(path, description)
      }
    dataOutput
  }

  @throws[IOException]
  def initializeFileSystem(path: Path, @Nullable extraUserConf: Configuration): FileSystem = {
    val fileSystem = path.getFileSystem
    require(fileSystem != null, "Cannot initialize Hadoop FileSystem with default config.")
    if (!fileSystem.isDistributedFS) {
      LOG.info(s"The path: ${path.toString} is not backed by Hadoop, will output to local.")
    }
    fileSystem
  }
}
