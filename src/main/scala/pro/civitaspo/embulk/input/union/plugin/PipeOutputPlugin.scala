package pro.civitaspo.embulk.input.union.plugin

import java.util.{List => JList}

import org.embulk.config.{
  ConfigDiff,
  ConfigSource,
  Task,
  TaskReport,
  TaskSource
}
import org.embulk.spi.{
  Exec,
  OutputPlugin,
  Page,
  PageOutput,
  Schema,
  TransactionalPageOutput
}

object PipeOutputPlugin {
  trait PluginTask extends Task
}

case class PipeOutputPlugin(output: PageOutput) extends OutputPlugin {

  override def transaction(
      config: ConfigSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = {
    val task = config.loadConfig(classOf[PipeOutputPlugin.PluginTask])
    control.run(task.dump())
    Exec.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff =
    throw new UnsupportedOperationException(
      "PipeOutputPlugin does not support 'resume'."
    )

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    // do nothing.
  }

  override def open(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int
  ): TransactionalPageOutput =
    new TransactionalPageOutput {
      override def add(page: Page): Unit = {
        output.add(page)
      }
      override def finish(): Unit = {
        // NOTE: The Original PageOutput will be closed by UnionInputPlugin.
        // output.finish()
      }
      override def close(): Unit = {
        // NOTE: InputPlugin must not close the Original PageOutput.
        // output.close()
      }
      override def abort(): Unit = {
        // do nothing
      }
      override def commit(): TaskReport = Exec.newTaskReport()
    }
}
