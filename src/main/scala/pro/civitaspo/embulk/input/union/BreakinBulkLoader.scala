package pro.civitaspo.embulk.input.union

import java.util.{Optional, List => JList}

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigDiff,
  ConfigException,
  ConfigSource,
  TaskSource,
  Task => EmbulkTask
}
import org.embulk.exec.TransactionStage
import org.embulk.exec.TransactionStage.{
  EXECUTOR_BEGIN,
  EXECUTOR_COMMIT,
  FILTER_BEGIN,
  FILTER_COMMIT,
  INPUT_BEGIN,
  INPUT_COMMIT,
  OUTPUT_BEGIN,
  OUTPUT_COMMIT,
  RUN
}
import org.embulk.plugin.PluginType
import org.embulk.spi.{
  Exec,
  ExecutorPlugin,
  FileInputRunner,
  FilterPlugin,
  InputPlugin,
  OutputPlugin,
  PageOutput,
  ProcessState,
  ProcessTask,
  Schema
}
import org.embulk.spi.util.Filters
import org.slf4j.{Logger, LoggerFactory}
import pro.civitaspo.embulk.input.union.plugin.{
  PipeOutputPlugin,
  ReuseOutputLocalExecutorPlugin
}

import scala.util.chaining._

object BreakinBulkLoader {
  trait Task extends EmbulkTask {
    @Config("name")
    @ConfigDefault("null")
    def getName: Optional[String]

    @Config("exec")
    @ConfigDefault("{}")
    def getExec: ConfigSource

    @Config("in")
    def getIn: ConfigSource

    @Config("filters")
    @ConfigDefault("[]")
    def getFilters: JList[ConfigSource]

    // NOTE: When embulk is run as a server, the bulk loads that have the same
    //       loaderName cannot run twice or more because LoaderState is shared.
    //       So, the transaction id is used to distinguish the bulk loads.
    def setTransactionId(execId: String): Unit
    def getTransactionId: String
  }

  case class Exception(
      name: String,
      transactionStage: Option[TransactionStage],
      cause: Throwable
  ) extends RuntimeException(cause)

  case class Result(
      configDiff: ConfigDiff,
      ignoredExceptions: Seq[Throwable]
  )
}

case class BreakinBulkLoader(task: BreakinBulkLoader.Task, idx: Int) {
  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[BreakinBulkLoader])

  private lazy val state: LoaderState = LoaderState.get(loaderName)
  private lazy val loaderName: String =
    s"transaction[${task.getTransactionId}]:union[$idx]:" + task.getName
      .getOrElse {
        s"in[${inputPluginType.getName}]" +
          s".filters[${filterPluginTypes.map(_.getName).mkString(",")}]"
      }

  private lazy val executorTask: ConfigSource = task.getExec
  private lazy val outputTask: ConfigSource = Exec.newConfigSource()

  private lazy val inputTask: ConfigSource = task.getIn
  private lazy val inputPluginType: PluginType =
    inputTask.get(classOf[PluginType], "type")
  private lazy val inputPlugin: InputPlugin =
    Exec.newPlugin(classOf[InputPlugin], inputPluginType)

  private lazy val filterTasks: Seq[ConfigSource] = task.getFilters
  private lazy val filterPluginTypes: Seq[PluginType] =
    Filters.getPluginTypes(filterTasks)
  private lazy val filterPlugins: Seq[FilterPlugin] =
    Filters.newFilterPlugins(Exec.session(), filterPluginTypes)

  def transaction(f: Schema => Unit): Unit = {
    ThreadNameContext.switch(s"$loaderName:#transaction") {
      ctxt: ThreadNameContext =>
        try {
          runInput {
            runFilters {
              // NOTE: This "f" calls "control.run" that is defined in UnionInputPlugin.
              //       And then "control.run" calls UnionInputPlugin#run. This means
              //       BreakinBulkLoader#run is called inside "f". So the plugins
              //       executions order become the same as the Embulk BulkLoader.
              //       ref. https://github.com/embulk/embulk/blob/c532e7c084ef7041914ec6b119522f6cb7dcf8e8/embulk-core/src/main/java/org/embulk/exec/BulkLoader.java#L498-L568
              ctxt.switch { _ => f(lastFilterSchema) }
            }
          }
        }
        catch {
          // NOTE: BreakinBulkLoader does not catch SkipTransactionException
          //       because this exception should be handled in the original
          //       BulkLoader to stop the output plugin ingesting.
          // NOTE: BreakinBulkLoader catch only the exception wrapped by
          //       BreakinBulkLoader.Exception that has this BreakinBulkLoader's
          //       name, because any other exceptions are not thrown by this
          //       BreakinBulkLoader.
          // NOTE: BreakinBulkLoader allows to suppress exceptions only when
          //       the all tasks and all transactions are committed.
          case ex: BreakinBulkLoader.Exception
              if (ex.name == loaderName && state.isAllTasksCommitted && state.isAllTransactionsCommitted) =>
            logger.warn(
              s"Threw exception on the stage: ${ex.transactionStage.map(_.name()).getOrElse("None")}," +
                s" but all tasks and transactions are committed.",
              ex
            )
        }
    }
  }

  def run(schema: Schema, output: PageOutput): Unit = {
    ThreadNameContext.switch(s"$loaderName:#run") { _ =>
      val outputPlugin = PipeOutputPlugin(output)
      val executorPlugin = ReuseOutputLocalExecutorPlugin(outputPlugin)
      try {
        runExecutor(executorPlugin, schema) {
          (executor: ExecutorPlugin.Executor) =>
            runOutput(outputPlugin) {
              execute(executor)
            }
        }
      }
      catch {
        // NOTE: Wrap the exception by BreakinBulkLoader.Exception
        //       in order to identify this exception thrown by this
        //       BreakinBulkLoader.
        case ex: Throwable => throw buildException(ex)
      }
    }
  }

  def cleanup(): Unit = {
    ThreadNameContext.switch(s"$loaderName:#cleanup") { _ =>
      val inputTaskSource: TaskSource = inputPlugin match {
        case _: FileInputRunner =>
          FileInputRunner.getFileInputTaskSource(state.getInputTaskSource.get)
        case _ => state.getInputTaskSource.get
      }
      inputPlugin.cleanup(
        inputTaskSource,
        state.getInputSchema.get,
        state.getInputTaskCount.get,
        state.getInputTaskReports.flatten
      )

      // NOTE: PipeOutputPlugin does not need to do cleanup.
      // val outputTaskSource: TaskSource = inputPlugin match {
      //   case _: FileOutputRunner =>
      //     FileOutputRunner.getFileOutputTaskSource(
      //       state.getOutputTaskSource.get
      //     )
      //   case _ => state.getOutputTaskSource.get
      // }
      // outputPlugin.cleanup(
      //   outputTaskSource,
      //   state.getExecutorSchema.get,
      //   state.getOutputTaskCount.get,
      //   state.getOutputTaskReports.flatten
      // )

      state.cleanup()
    }
  }

  def getResult: BreakinBulkLoader.Result = buildResult()

  private def lastFilterSchema: Schema =
    state.getFilterSchemas.map(_.last).getOrElse {
      throw new ConfigException(
        "'filterSchemas' must be set. Call #runFilters before."
      )
    }

  private def buildException(
      ex: Throwable
  ): BreakinBulkLoader.Exception = {
    BreakinBulkLoader.Exception(
      loaderName,
      state.getTransactionStage,
      ex
    )
  }

  private def buildResult(): BreakinBulkLoader.Result = {
    BreakinBulkLoader.Result(
      configDiff = Exec.newConfigDiff().tap { configDiff: ConfigDiff =>
        state.getInputConfigDiff.foreach(configDiff.setNested("in", _))
      // NOTE: BreakinBulkLoader does not support PipeOutputPlugin configuration.
      // state.getOutputConfigDiff.foreach(configDiff.setNested("out", _))
      },
      ignoredExceptions = state.getExceptions
    )
  }

  private def newProcessTask: ProcessTask = {
    new ProcessTask(
      inputPluginType,
      null, // NOTE: Embulk Plugin Manager does not have PipeOutputPlugin.
      filterPluginTypes,
      state.getInputTaskSource.get,
      state.getOutputTaskSource.get,
      state.getFilterTaskSources.get,
      state.getFilterSchemas.get,
      state.getExecutorSchema.get,
      Exec.newTaskSource()
    )
  }

  // scalafmt: { maxColumn = 130 }
  private def runInput(f: => Unit): Unit = {
    state.setTransactionStage(INPUT_BEGIN)
    val inputControl: InputPlugin.Control =
      (inputTaskSource: TaskSource, inputSchema: Schema, inputTaskCount: Int) => {
        state.setInputSchema(inputSchema)
        state.setInputTaskSource(inputTaskSource)
        state.setInputTaskCount(inputTaskCount)
        f
        state.setTransactionStage(INPUT_COMMIT)
        state.getAllInputTaskReports
      }
    val inputConfigDiff: ConfigDiff = inputPlugin.transaction(inputTask, inputControl)
    state.setInputConfigDiff(inputConfigDiff)
  }

  private def runFilters(f: => Unit): Unit = {
    val inputSchema: Schema = state.getInputSchema.getOrElse {
      throw new ConfigException("'inputSchema' must be set. Call #runInput before.")
    }
    state.setTransactionStage(FILTER_BEGIN)
    val filtersControl: Filters.Control =
      (filterTaskSources: JList[TaskSource], filterSchemas: JList[Schema]) => {
        state.setFilterTaskSources(filterTaskSources)
        state.setFilterSchemas(filterSchemas)
        f
        state.setTransactionStage(FILTER_COMMIT)
      }
    Filters.transaction(filterPlugins, filterTasks, inputSchema, filtersControl)
  }

  private def runExecutor(executorPlugin: ExecutorPlugin, schema: Schema)(
      f: ExecutorPlugin.Executor => Unit
  ): Unit = {
    val inputTaskCount: Int = state.getInputTaskCount.getOrElse {
      throw new ConfigException("'inputTaskCount' must be set. Call #runInput before.")
    }
    state.setTransactionStage(EXECUTOR_BEGIN)
    val executorControl: ExecutorPlugin.Control =
      (executorSchema: Schema, outputTaskCount: Int, executor: ExecutorPlugin.Executor) => {
        state.setExecutorSchema(executorSchema)
        state.setOutputTaskCount(outputTaskCount)
        f(executor)
        state.setTransactionStage(EXECUTOR_COMMIT)
      }
    executorPlugin.transaction(executorTask, schema, inputTaskCount, executorControl)
  }

  private def runOutput(outputPlugin: OutputPlugin)(
      f: => Unit
  ): Unit = {
    val executorSchema: Schema = state.getExecutorSchema.getOrElse {
      throw new ConfigException("'executorSchema' must be set. Call #runExecutor before.")
    }
    val outputTaskCount: Int = state.getOutputTaskCount.getOrElse {
      throw new ConfigException("'outputTaskCount' must be set. Call #runExecutor before.")
    }
    state.setTransactionStage(OUTPUT_BEGIN)
    val outputControl: OutputPlugin.Control =
      (outputTaskSource: TaskSource) => {
        state.setOutputTaskSource(outputTaskSource)
        f
        state.setTransactionStage(OUTPUT_COMMIT)
        state.getAllOutputTaskReports
      }
    val outputConfigDiff: ConfigDiff =
      outputPlugin.transaction(outputTask, executorSchema, outputTaskCount, outputControl)
    state.setOutputConfigDiff(outputConfigDiff)
  }

  private def execute(executor: ExecutorPlugin.Executor): Unit = {
    val processState: ProcessState = state.newProcessState
    processState.initialize(state.getInputTaskCount.get, state.getOutputTaskCount.get)
    state.setTransactionStage(RUN)
    if (!state.isAllTasksCommitted) {
      executor.execute(newProcessTask, processState)
      if (!state.isAllTasksCommitted) throw state.buildRepresentativeException
    }
    if (!state.isAllTasksCommitted) {
      throw new RuntimeException(
        s"${state.countUncommittedInputTasks} input tasks" +
          s" and ${state.countUncommittedOutputTasks} output tasks failed."
      )
    }
  }
  // scalafmt: { maxColumn = 80 }
}
