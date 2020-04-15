package pro.civitaspo.embulk.input.union

import java.util.{List => JList}

import org.embulk.config.{
  Config,
  ConfigDiff,
  ConfigSource,
  Task,
  TaskReport,
  TaskSource
}
import org.embulk.spi.{Exec, InputPlugin, PageOutput, Schema, SchemaConfig}
import pro.civitaspo.embulk.input.union.loader.UnionBulkLoader

import scala.util.Using

class UnionInputPlugin extends InputPlugin {
  trait PluginTask extends Task {
    @Config("union")
    def getUnion: JList[UnionBulkLoader.Task]

    @Config("columns")
    def getColumns: SchemaConfig
  }

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])
    val schema: Schema = task.getColumns.toSchema
    val taskCount: Int = task.getUnion.size()

    val taskReports: JList[TaskReport] =
      control.run(task.dump(), schema, taskCount)
    Exec.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff =
    throw new UnsupportedOperationException(
      "UnionInputPlugin does not support 'resume'."
    )

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {
    // do nothing.
  }

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
    Using.resource(
      UnionBulkLoader(
        task.getUnion.get(taskIndex),
        taskIndex,
        output
      )
    ) { loader => loader.load() }
    Exec.newTaskReport()
  }
  override def guess(config: ConfigSource): ConfigDiff =
    throw new UnsupportedOperationException(
      "UnionInputPlugin does not support 'guess'."
    )
}
