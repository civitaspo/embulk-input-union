package pro.civitaspo.embulk.input.union

import java.util.{Optional, List => JList}

import org.embulk.config.{
  Config,
  ConfigDiff,
  ConfigException,
  ConfigSource,
  Task,
  TaskReport,
  TaskSource
}
import org.embulk.spi.{Exec, InputPlugin, PageOutput, Schema}

import scala.util.chaining._

class UnionInputPlugin extends InputPlugin {
  import implicits._

  trait PluginTask extends Task {
    @Config("union")
    def getUnion: JList[BreakinBulkLoader.Task]
  }

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])
    if (task.getUnion.isEmpty)
      throw new ConfigException(
        "1 or more configurations are required for \"union\" option."
      )

    val transactionId: String = Utils.genTransactionId()
    task.getUnion.foreach(_.setTransactionId(transactionId))

    val runControlCallback: Schema => Unit = { (schema: Schema) =>
      // NOTE: UnionInputPlugin#run does not return any TaskReport.
      //       So, the return values are thrown away.
      control.run(task.dump(), schema, task.getUnion.size)
    }

    val loaders: Seq[BreakinBulkLoader] = task.getUnion.zipWithIndex.map {
      case (loaderTask: BreakinBulkLoader.Task, idx: Int) =>
        BreakinBulkLoader(loaderTask, idx)
    }
    loaders.head.transaction(loaders.tail.foldLeft(runControlCallback) {
      (callback: Schema => Unit, nextLoader: BreakinBulkLoader) =>
        { (schema: Schema) =>
          nextLoader.transaction { nextSchema: Schema =>
            if (!schema.equals(nextSchema)) {
              throw new ConfigException(
                s"Different schema is found: ${schema.toString} != ${nextSchema.toString}"
              )
            }
            callback(nextSchema)
          }
        }
    })
    Exec.newConfigDiff().tap { configDiff: ConfigDiff =>
      val unionConfigDiffs: JList[ConfigDiff] =
        loaders.map(_.getResult.configDiff)
      configDiff.set("union", unionConfigDiffs)
    }
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
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
    task.getUnion.zipWithIndex.foreach {
      case (loaderTask: BreakinBulkLoader.Task, idx: Int) =>
        BreakinBulkLoader(loaderTask, idx).cleanup()
    }
  }

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
    val loaderTask: BreakinBulkLoader.Task = task.getUnion(taskIndex)

    try BreakinBulkLoader(loaderTask, taskIndex).run(schema, output)
    finally {
      // NOTE: `embulk preview` shows the below error when output.finish() is executed.
      // 2022-03-07 14:48:01.939 +0000 [ERROR] (0001:preview): PreviewResult recreation will cause a bug. The plugin must call PageOutput#finish() only once.
      if (ThreadNameContext.isPreviewExecution) {
        if (Utils.shouldFinishSamplingPageOutput(output)) output.finish()
      }
      else output.finish()
    }
    Exec.newTaskReport()
  }
  override def guess(config: ConfigSource): ConfigDiff =
    throw new UnsupportedOperationException(
      "UnionInputPlugin does not support 'guess'."
    )
}
