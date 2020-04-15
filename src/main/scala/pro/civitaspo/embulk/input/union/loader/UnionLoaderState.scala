package pro.civitaspo.embulk.input.union.loader

import java.util.{List => JList}

import com.google.common.base.Optional
import org.embulk.config.{ConfigDiff, ConfigException, TaskReport, TaskSource}
import org.embulk.exec.{ExecutionResult, PartialExecutionException, ResumeState, TransactionStage}
import org.embulk.spi.{Exec, ExecSession, ProcessState, Schema, TaskState}

import scala.jdk.CollectionConverters._
import scala.util.chaining._

object UnionLoaderState {
  private case class State(
      inputTaskCount: Option[Int] = None,
      outputTaskCount: Option[Int] = None,
      inputTaskStates: Option[Seq[TaskState]] = None,
      outputTaskStates: Option[Seq[TaskState]] = None,
      inputConfigDiff: Option[ConfigDiff] = None,
      outputConfigDiff: Option[ConfigDiff] = None,
      transactionStage: Option[TransactionStage] = None,
      inputTaskSource: Option[TaskSource] = None,
      filterTaskSources: Option[Seq[TaskSource]] = None,
      outputTaskSource: Option[TaskSource] = None,
      executorSchema: Option[Schema] = None,
      schemas: Option[Seq[Schema]] = None
  )

  def apply(): UnionLoaderState = {
    new UnionLoaderState(State())
  }
}

class UnionLoaderState private (private var state: UnionLoaderState.State) extends ProcessState {
  override def initialize(inputTaskCount: Int, outputTaskCount: Int): Unit = {
    if (state.inputTaskStates.isDefined || state.outputTaskStates.isDefined) {
      if (state.inputTaskStates.get.size != inputTaskCount || state.outputTaskStates.get.size != outputTaskCount) {
        throw new ConfigException(
          s"input task count and output task ($inputTaskCount and $outputTaskCount) must be same " +
            s"with the first execution (${state.inputTaskStates.get.size} " +
            s"and ${state.outputTaskStates.get.size}) where resumed"
        )
      }
      return
    }
    state = state.copy(
      inputTaskCount = Option(inputTaskCount),
      outputTaskCount = Option(outputTaskCount),
      inputTaskStates = Option((0 until inputTaskCount).map(_ => new TaskState())),
      outputTaskStates = Option((0 until outputTaskCount).map(_ => new TaskState()))
    )
  }
  override def getInputTaskState(inputTaskIndex: Int): TaskState =
    state.inputTaskStates.get(inputTaskIndex)
  override def getOutputTaskState(outputTaskIndex: Int): TaskState =
    state.outputTaskStates.get(outputTaskIndex)
  def setTransactionStage(transactionStage: TransactionStage): Unit =
    state = state.copy(transactionStage = Option(transactionStage))
  def setInputTaskSource(inputTaskSource: TaskSource): Unit =
    state = state.copy(inputTaskSource = Option(inputTaskSource))
  def getInputTaskSource: TaskSource = state.inputTaskSource.orNull
  def setOutputTaskSource(outputTaskSource: TaskSource): Unit =
    state = state.copy(outputTaskSource = Option(outputTaskSource))
  def getOutputTaskSource: TaskSource = state.outputTaskSource.orNull
  def setSchemas(schemas: Seq[Schema]): Unit =
    state = state.copy(schemas = Option(schemas))
  def setSchemas(schemas: JList[Schema]): Unit = setSchemas(schemas.asScala.toSeq)
  def getInputSchema: Schema = state.schemas.map(_.head).orNull
  def setFilterTaskSources(filterTaskSources: Seq[TaskSource]): Unit =
    state = state.copy(filterTaskSources = Option(filterTaskSources))
  def setFilterTaskSources(filterTaskSources: JList[TaskSource]): Unit =
    setFilterTaskSources(filterTaskSources.asScala.toSeq)
  def setExecutorSchema(executorSchema: Schema): Unit =
    state = state.copy(executorSchema = Option(executorSchema))
  def getOutputSchema: Schema = state.executorSchema.orNull
  def isAllTasksCommitted: Boolean = {
    for (inStates <- state.inputTaskStates; outStates <- state.outputTaskStates) {
      for (s <- inStates if !s.isCommitted) return false
      for (s <- outStates if !s.isCommitted) return false
      return true
    }
    false
  }
  def countUncommittedInputTasks: Int =
    state.inputTaskStates.map(_.count(!_.isCommitted)).getOrElse(0)
  def countUncommittedOutputTasks: Int =
    state.outputTaskStates.map(_.count(!_.isCommitted)).getOrElse(0)
  def isAllTransactionsCommitted: Boolean =
    state.inputConfigDiff.isDefined && state.outputConfigDiff.isDefined
  def setInputConfigDiff(inputConfigDiff: ConfigDiff): Unit =
    state =
      state.copy(inputConfigDiff = Option(inputConfigDiff).orElse(Option(Exec.newConfigDiff())))
  def setOutputConfigDiff(outputConfigDiff: ConfigDiff): Unit =
    state =
      state.copy(outputConfigDiff = Option(outputConfigDiff).orElse(Option(Exec.newConfigDiff())))
  def getInputTaskReports: Seq[Option[TaskReport]] =
    state.inputTaskStates.get.map(s => Option(s.getTaskReport.orNull()))
  def getOutputTaskReports: Seq[Option[TaskReport]] =
    state.outputTaskStates.get.map(s => Option(s.getTaskReport.orNull()))
  def getAllInputTaskReports: Seq[TaskReport] = getInputTaskReports.map(_.get)
  def getAllOutputTaskReports: Seq[TaskReport] = getOutputTaskReports.map(_.get)
  def getExceptions: Seq[Throwable] =
    (state.inputTaskStates.getOrElse(Seq.empty) ++ state.outputTaskStates.getOrElse(Seq.empty))
      .filter(_.getException.isPresent)
      .map(_.getException.get())
  def getRepresentativeException: RuntimeException = getExceptions match {
    case head :: tail =>
      (head match {
        case ex: RuntimeException => ex
        case ex                   => new RuntimeException(ex)
      }).tap(topEx => tail.foreach(topEx.addSuppressed))
  }
  def buildExecutionResult(): ExecutionResult = buildExecutionResultWithWarningException()
  def buildExecutionResultWithWarningException(ex: Option[Throwable] = None): ExecutionResult = {
    val configDiff: ConfigDiff = Exec
      .newConfigDiff()
      .tap { configDiff =>
        state.inputConfigDiff.foreach(x => configDiff.getNestedOrSetEmpty("in").merge(x))
      }
      .tap { configDiff =>
        state.outputConfigDiff.foreach(x => configDiff.getNestedOrSetEmpty("out").merge(x))
      }
    val ignoredExceptions = ex.foldLeft(getExceptions) { (exceptions, ex) => exceptions :+ ex }
    new ExecutionResult(configDiff, false, ignoredExceptions.asJava)
  }
  def buildExecutionResultOfSkippedExecution(configDiff: ConfigDiff): ExecutionResult =
    new ExecutionResult(configDiff, true, getExceptions.asJava)
  def buildResumeState(exec: ExecSession): ResumeState =
    new ResumeState(
      exec.getSessionExecConfig,
      state.inputTaskSource.orNull,
      state.outputTaskSource.orNull,
      state.schemas.map(_.head).orNull,
      state.executorSchema.orNull,
      state.inputTaskStates
        .map(_ => getInputTaskReports.map(r => Optional.fromNullable(r.orNull)).asJava)
        .orNull,
      state.outputTaskStates
        .map(_ => getOutputTaskReports.map(r => Optional.fromNullable(r.orNull)).asJava)
        .orNull
    )
  def buildPartialExecuteException(cause: Throwable, exec: ExecSession): PartialExecutionException =
    new PartialExecutionException(cause, buildResumeState(exec), state.transactionStage.orNull)
}
