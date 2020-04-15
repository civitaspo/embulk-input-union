package pro.civitaspo.embulk.input.union.loader

import java.util.{List => JList}
import java.util.concurrent.Future

import org.embulk.config.TaskReport
import org.embulk.exec.LocalExecutorPlugin.DirectExecutor
import org.embulk.exec.SetCurrentThreadName
import org.embulk.spi.{
  Exec,
  ExecSession,
  FilterPlugin,
  InputPlugin,
  OutputPlugin,
  ProcessState,
  ProcessTask
}
import org.embulk.spi.util.{Executors, Filters}
import org.embulk.spi.util.Executors.ProcessStateCallback
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Using

class ReuseOutputLocalExecutor(
    outputPlugin: OutputPlugin,
    maxThreads: Int,
    taskCount: Int
) extends DirectExecutor(maxThreads, taskCount) {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[ReuseOutputLocalExecutor])

  override def startInputTask(
      task: ProcessTask,
      state: ProcessState,
      taskIndex: Int
  ): Future[Throwable] = {
    if (state.getOutputTaskState(taskIndex).isCommitted) {
      logger.warn("Skipped resumed task {}", taskIndex)
      return null // resumed
    }

    executor.submit(() => {
      try {
        withCurrentThreadName(taskIndex) {
          val callback = new ProcessStateCallback {
            override def started(): Unit = {
              state.getInputTaskState(taskIndex).start()
              state.getOutputTaskState(taskIndex).start()
            }

            override def inputCommitted(report: TaskReport): Unit =
              state.getInputTaskState(taskIndex).setTaskReport(report)

            override def outputCommitted(report: TaskReport): Unit =
              state.getOutputTaskState(taskIndex).setTaskReport(report)
          }
          process(Exec.session(), task, taskIndex, callback)
        }
        null
      }
      finally {
        state.getInputTaskState(taskIndex).finish()
        state.getOutputTaskState(taskIndex).finish()
      }
    })
  }

  private def withCurrentThreadName[A](taskIndex: Int)(f: => A): A = {
    Using.resource(
      new SetCurrentThreadName(String.format("task-%04d", taskIndex))
    ) { _ => f }
  }

  private def process(
      execSession: ExecSession,
      task: ProcessTask,
      taskIndex: Int,
      callback: ProcessStateCallback
  ): Unit = {
    val inputPlugin: InputPlugin =
      execSession.newPlugin(classOf[InputPlugin], task.getInputPluginType)
    val filterPlugins: JList[FilterPlugin] =
      Filters.newFilterPlugins(execSession, task.getFilterPluginTypes)
    Executors.process(
      execSession,
      taskIndex,
      inputPlugin,
      task.getInputSchema,
      task.getInputTaskSource,
      filterPlugins,
      task.getFilterSchemas,
      task.getFilterTaskSources,
      outputPlugin,
      task.getOutputSchema,
      task.getOutputTaskSource,
      callback
    )
  }
}
