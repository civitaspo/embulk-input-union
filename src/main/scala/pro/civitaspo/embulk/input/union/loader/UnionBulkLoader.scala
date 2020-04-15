package pro.civitaspo.embulk.input.union.loader

import java.util.{List => JList}

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigDiff,
  ConfigSource,
  TaskReport,
  TaskSource,
  Task => EmbulkTask
}
import org.embulk.exec.{
  ExecutionResult,
  ResumeState,
  SetCurrentThreadName,
  SkipTransactionException,
  TransactionStage
}
import org.embulk.plugin.PluginType
import org.embulk.spi.{
  Exec,
  ExecutorPlugin,
  FileInputRunner,
  FileOutputRunner,
  FilterPlugin,
  InputPlugin,
  OutputPlugin,
  PageOutput,
  ProcessTask,
  Schema
}
import org.embulk.spi.util.Filters
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.Using

object UnionBulkLoader {
  trait Task extends EmbulkTask {
    @Config("exec")
    @ConfigDefault("{}")
    def getExec: ConfigSource

    @Config("in")
    def getIn: ConfigSource

    @Config("filters")
    @ConfigDefault("[]")
    def getFilters: JList[ConfigSource]
  }
}

case class UnionBulkLoader(
    task: UnionBulkLoader.Task,
    taskIndex: Int,
    output: PageOutput
) extends AutoCloseable {
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  lazy val executorPlugin: ExecutorPlugin = ReuseOutputLocalExecutorPlugin(
    outputPlugin
  )

  lazy val inputPluginType: PluginType =
    task.getIn.get(classOf[PluginType], "type")
  lazy val inputPlugin: InputPlugin =
    Exec.newPlugin(classOf[InputPlugin], inputPluginType)

  lazy val outputPlugin: OutputPlugin = PipeOutputPlugin(output)

  lazy val filterPluginTypes: JList[PluginType] =
    Filters.getPluginTypes(task.getFilters)
  lazy val filterPlugins: JList[FilterPlugin] =
    Filters.newFilterPlugins(Exec.session(), filterPluginTypes)

  private def withCurrentThreadName[A](f: => A): A = {
    Using.resource(
      new SetCurrentThreadName(String.format("UnionBulkLoader-%04d", taskIndex))
    ) { _ => f }
  }

  def load(): Unit = {
    withCurrentThreadName {
      val result: ExecutionResult = load(UnionLoaderState())
      logger.info("Committed.")
      logger.info(s"Next config diff: ${result.getConfigDiff}")
    }
  }

  private def load(state: UnionLoaderState): ExecutionResult = {
    try {
      // scalafmt: { maxColumn = 130 }
      runInput(state) { (inputTaskSource: TaskSource, inputSchema: Schema, inputTaskCount: Int) =>
        runFilters(state, inputSchema) { (filterTaskSources: Seq[TaskSource], filterSchemas: Seq[Schema]) =>
          runExecutor(state, filterSchemas.last, inputTaskCount) {
            (executorSchema: Schema, outputTaskCount: Int, executor: ExecutorPlugin.Executor) =>
              runOutput(state, executorSchema, outputTaskCount) { outputTaskSource: TaskSource =>
                state.initialize(inputTaskCount, outputTaskCount)
                state.setTransactionStage(TransactionStage.RUN)
                if (!state.isAllTasksCommitted) {
                  val processTask = newProcessTask(
                    inputTaskSource,
                    outputTaskSource,
                    filterTaskSources,
                    filterSchemas,
                    executorSchema
                  )
                  executor.execute(processTask, state)
                  if (!state.isAllTasksCommitted)
                    throw state.getRepresentativeException
                }
                if (!state.isAllTasksCommitted)
                  throw new RuntimeException(
                    s"${state.countUncommittedInputTasks} input tasks" +
                      s" and ${state.countUncommittedOutputTasks} output tasks failed."
                  )
              }
          }
        }
      }
      // scalafmt: { maxColumn = 80 } (back to default)
      runCleanup(state)
      state.buildExecutionResult()
    }
    catch {
      case ex: SkipTransactionException =>
        state.buildExecutionResultOfSkippedExecution(ex.getConfigDiff)
      case ex: Throwable
          if (state.isAllTasksCommitted && state.isAllTransactionsCommitted) =>
        // ignore the exception
        state.buildExecutionResultWithWarningException(Option(ex))
      case ex: Throwable =>
        throw state.buildPartialExecuteException(ex, Exec.session())
    }
  }

  private def runInput(
      state: UnionLoaderState
  )(f: (TaskSource, Schema, Int) => Unit): Unit = {
    state.setTransactionStage(TransactionStage.INPUT_BEGIN)
    val inputConfigDiff: ConfigDiff = inputPlugin.transaction(
      task.getIn,
      (inputTaskSource: TaskSource, inputSchema: Schema, inputTaskCount: Int) =>
        {
          state.setInputTaskSource(inputTaskSource)
          f(inputTaskSource, inputSchema, inputTaskCount)
          state.setTransactionStage(TransactionStage.INPUT_COMMIT)
          state.getAllInputTaskReports.asJava
        }
    )
    state.setInputConfigDiff(inputConfigDiff)
  }

  private def runFilters(state: UnionLoaderState, inputSchema: Schema)(
      f: (Seq[TaskSource], Seq[Schema]) => Unit
  ): Unit = {
    state.setTransactionStage(TransactionStage.FILTER_BEGIN)
    Filters.transaction(
      filterPlugins,
      task.getFilters,
      inputSchema,
      (filterTaskSources: JList[TaskSource], filterSchemas: JList[Schema]) => {
        state.setSchemas(filterSchemas)
        state.setFilterTaskSources(filterTaskSources)
        f(filterTaskSources.asScala.toSeq, filterSchemas.asScala.toSeq)
        state.setTransactionStage(TransactionStage.FILTER_COMMIT)
      }
    )
  }
  private def runExecutor(
      state: UnionLoaderState,
      lastFilterSchema: Schema,
      inputTaskCount: Int
  )(
      f: (Schema, Int, ExecutorPlugin.Executor) => Unit
  ): Unit = {
    state.setTransactionStage(TransactionStage.EXECUTOR_BEGIN)
    executorPlugin.transaction(
      task.getExec,
      lastFilterSchema,
      inputTaskCount,
      (
          executorSchema: Schema,
          outputTaskCount: Int,
          executor: ExecutorPlugin.Executor
      ) => {
        state.setExecutorSchema(executorSchema)
        f(executorSchema, outputTaskCount, executor)
        state.setTransactionStage(TransactionStage.EXECUTOR_COMMIT)
      }
    )
  }

  private def runOutput(
      state: UnionLoaderState,
      executorSchema: Schema,
      outputTaskCount: Int
  )(
      f: TaskSource => Unit
  ): Unit = {
    state.setTransactionStage(TransactionStage.OUTPUT_BEGIN)
    val outputConfigDiff: ConfigDiff = outputPlugin.transaction(
      Exec.newConfigSource(),
      executorSchema,
      outputTaskCount,
      (outputTaskSource: TaskSource) => {
        state.setOutputTaskSource(outputTaskSource)
        f(outputTaskSource)
        state.setTransactionStage(TransactionStage.OUTPUT_COMMIT)
        state.getAllOutputTaskReports.asJava
      }
    )
    state.setOutputConfigDiff(outputConfigDiff)
  }

  private def runCleanup(state: UnionLoaderState): Unit = {
    state.setTransactionStage(TransactionStage.CLEANUP)
    try {
      val resumeState: ResumeState = state.buildResumeState(Exec.session())
      val successfulInputTaskReports: Seq[TaskReport] =
        state.getInputTaskReports.filter(_.isDefined).map(_.get)
      val successfulOutputTaskReports: Seq[TaskReport] =
        state.getOutputTaskReports.filter(_.isDefined).map(_.get)
      val inputTaskSource = inputPlugin match {
        case _: FileInputRunner =>
          FileInputRunner.getFileInputTaskSource(state.getInputTaskSource)
        case _ => state.getInputTaskSource
      }
      inputPlugin.cleanup(
        inputTaskSource,
        state.getInputSchema,
        state.getInputTaskReports.size,
        successfulInputTaskReports.asJava
      )
      val outputTaskSource = outputPlugin match {
        case _: FileOutputRunner =>
          FileOutputRunner.getFileOutputTaskSource(state.getOutputTaskSource)
        case _ => state.getOutputTaskSource
      }
      outputPlugin.cleanup(
        outputTaskSource,
        state.getOutputSchema,
        state.getOutputTaskReports.size,
        successfulOutputTaskReports.asJava
      )
    }
    catch {
      case ex: Exception =>
        logger.warn(
          "Commit succeeded but cleanup failed. Ignoring this exception.",
          ex
        )
    }
  }

  private def newProcessTask(
      inputTaskSource: TaskSource,
      outputTaskSource: TaskSource,
      filterTaskSources: Seq[TaskSource],
      schemas: Seq[Schema],
      executorSchema: Schema
  ): ProcessTask = {
    new ProcessTask(
      inputPluginType,
      null,
      filterPluginTypes,
      inputTaskSource,
      outputTaskSource,
      filterTaskSources.asJava,
      schemas.asJava,
      executorSchema,
      Exec.newTaskSource()
    )
  }

  override def close(): Unit = {
    output.close()
  }
}
