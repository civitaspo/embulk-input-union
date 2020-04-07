package org.embulk.input.union

import java.util.{List => JList}

import com.google.common.base.Optional

import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.Task
import org.embulk.config.TaskReport
import org.embulk.config.TaskSource
import org.embulk.spi.Exec
import org.embulk.spi.InputPlugin
import org.embulk.spi.PageOutput
import org.embulk.spi.Schema
import org.embulk.spi.SchemaConfig

class UnionInputPlugin extends InputPlugin {

  trait PluginTask extends Task {

    // configuration option 1 (required integer)
    @Config("option1")
    def getOption1(): Int

    // configuration option 2 (optional string, null is not allowed)
    @Config("option2")
    @ConfigDefault("\"myvalue\"")
    def getOption2(): String

    // configuration option 3 (optional string, null is allowed)
    @Config("option3")
    @ConfigDefault("null")
    def getOption3(): Optional[String]

    // if you get schema from config
    @Config("columns")
    def getColumns(): SchemaConfig
  }

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])

    val schema: Schema = task.getColumns().toSchema()
    val taskCount: Int = 1 // number of run() method calls

    resume(task.dump(), schema, taskCount, control)
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = {
    control.run(taskSource, schema, taskCount)
    Exec.newConfigDiff()
  }

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: JList[TaskReport]
  ): Unit = {}

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])

    // Write your code here :)
    throw new UnsupportedOperationException(
      "UnionInputPlugin.run method is not implemented yet"
    )
  }

  override def guess(config: ConfigSource): ConfigDiff = {
    Exec.newConfigDiff()
  }
}