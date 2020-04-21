package pro.civitaspo.embulk.input.union

import com.google.common.collect.Maps
import org.embulk.config.{ConfigDiff, ConfigException, TaskReport, TaskSource}
import org.embulk.exec.TransactionStage
import org.embulk.spi.{ProcessState, Schema, TaskState}

import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.jdk.CollectionConverters._

object LoaderState {

  case class State(
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
      inputSchema: Option[Schema] = None,
      filterSchemas: Option[Seq[Schema]] = None,
      executorSchema: Option[Schema] = None
  )

  private val states: ConcurrentMap[String, State] =
    Maps.newConcurrentMap[String, State]().asScala

  def get(name: String): LoaderState = {
    new LoaderState {
      override protected def getState: State =
        states.getOrElseUpdate(name, State())
      override protected def setState(state: State): Unit =
        states.update(name, state)
    }
  }
}

trait LoaderState {
  import implicits._

  protected def getState: LoaderState.State
  protected def setState(state: LoaderState.State): Unit

  def getInputTaskCount: Option[Int] = getState.inputTaskCount
  def setInputTaskCount(inputTaskCount: Int): Unit =
    setState(getState.copy(inputTaskCount = Option(inputTaskCount)))
  def getOutputTaskCount: Option[Int] = getState.outputTaskCount
  def setOutputTaskCount(outputTaskCount: Int): Unit =
    setState(getState.copy(outputTaskCount = Option(outputTaskCount)))
  def getInputTaskSource: Option[TaskSource] = getState.inputTaskSource
  def setInputTaskSource(inputTaskSource: TaskSource): Unit =
    setState(getState.copy(inputTaskSource = Option(inputTaskSource)))
  def getFilterTaskSources: Option[Seq[TaskSource]] = getState.filterTaskSources
  def setFilterTaskSources(filterTaskSources: Seq[TaskSource]): Unit =
    setState(getState.copy(filterTaskSources = Option(filterTaskSources)))
  def getOutputTaskSource: Option[TaskSource] = getState.outputTaskSource
  def setOutputTaskSource(outputTaskSource: TaskSource): Unit =
    setState(getState.copy(outputTaskSource = Option(outputTaskSource)))
  def getTransactionStage: Option[TransactionStage] = getState.transactionStage
  def setTransactionStage(transactionStage: TransactionStage): Unit =
    setState(getState.copy(transactionStage = Option(transactionStage)))
  def getInputConfigDiff: Option[ConfigDiff] = getState.inputConfigDiff
  def setInputConfigDiff(inputConfigDiff: ConfigDiff): Unit =
    setState(getState.copy(inputConfigDiff = Option(inputConfigDiff)))
  def getOutputConfigDiff: Option[ConfigDiff] = getState.outputConfigDiff
  def setOutputConfigDiff(outputConfigDiff: ConfigDiff): Unit =
    setState(getState.copy(outputConfigDiff = Option(outputConfigDiff)))
  def getInputTaskStates: Option[Seq[TaskState]] = getState.inputTaskStates
  def setInputTaskStates(inputTaskStates: Seq[TaskState]): Unit =
    setState(getState.copy(inputTaskStates = Option(inputTaskStates)))
  def getOutputTaskStates: Option[Seq[TaskState]] = getState.outputTaskStates
  def setOutputTaskStates(outputTaskStates: Seq[TaskState]): Unit =
    setState(getState.copy(outputTaskStates = Option(outputTaskStates)))
  def getInputSchema: Option[Schema] = getState.inputSchema
  def setInputSchema(inputSchema: Schema): Unit =
    setState(getState.copy(inputSchema = Option(inputSchema)))
  def getFilterSchemas: Option[Seq[Schema]] = getState.filterSchemas
  def setFilterSchemas(filterSchemas: Seq[Schema]): Unit =
    setState(getState.copy(filterSchemas = Option(filterSchemas)))
  def getExecutorSchema: Option[Schema] = getState.executorSchema
  def setExecutorSchema(executorSchema: Schema): Unit =
    setState(getState.copy(executorSchema = Option(executorSchema)))

  def newProcessState: ProcessState = {
    new ProcessState {
      override def initialize(
          inputTaskCount: Int,
          outputTaskCount: Int
      ): Unit = {
        if (getInputTaskStates.isDefined || getOutputTaskStates.isDefined) {
          if (getInputTaskStates.get.size != inputTaskCount || getOutputTaskStates.get.size != outputTaskCount) {
            throw new ConfigException(
              s"input count and output ($inputTaskCount and $outputTaskCount) must be same " +
                s"with the first execution (${getInputTaskStates.get.size} " +
                s"and ${getOutputTaskStates.get.size}) where resumed"
            )
          }
          return
        }
        setInputTaskStates(
          (0 until inputTaskCount).map(_ => new TaskState())
        )
        setOutputTaskStates(
          (0 until outputTaskCount).map(_ => new TaskState())
        )
      }
      override def getInputTaskState(inputTaskIndex: Int): TaskState =
        getInputTaskStates.get(inputTaskIndex)
      override def getOutputTaskState(outputTaskIndex: Int): TaskState =
        getOutputTaskStates.get(outputTaskIndex)
    }
  }
  def getInputTaskReports: Seq[Option[TaskReport]] =
    getInputTaskStates.get.map(_.getTaskReport)
  def getOutputTaskReports: Seq[Option[TaskReport]] =
    getOutputTaskStates.get.map(_.getTaskReport)
  def getAllInputTaskReports: Seq[TaskReport] =
    getInputTaskReports.map(_.get)
  def getAllOutputTaskReports: Seq[TaskReport] =
    getOutputTaskReports.map(_.get)
  def isAllTasksCommitted: Boolean = {
    for (inStates <- getInputTaskStates;
         outStates <- getOutputTaskStates) {
      for (s <- inStates if !s.isCommitted) return false
      for (s <- outStates if !s.isCommitted) return false
      return true
    }
    false
  }
  def isAllTransactionsCommitted: Boolean =
    getInputConfigDiff.isDefined && getOutputConfigDiff.isDefined
  def countUncommittedInputTasks: Int =
    getInputTaskStates.map(_.count(!_.isCommitted)).getOrElse(0)
  def countUncommittedOutputTasks: Int =
    getOutputTaskStates.map(_.count(!_.isCommitted)).getOrElse(0)

  def getExceptions: Seq[Throwable] =
    (getInputTaskStates ++ getOutputTaskStates)
      .flatMap(_.flatMap(_.getException))
      .toSeq
  def buildRepresentativeException: RuntimeException = {
    val head :: tail = getExceptions
    val topException: RuntimeException = head match {
      case ex: RuntimeException => ex
      case _                    => new RuntimeException(head)
    }
    tail.foreach(topException.addSuppressed)
    topException
  }
}