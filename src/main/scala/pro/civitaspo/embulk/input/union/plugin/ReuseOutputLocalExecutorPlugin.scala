package pro.civitaspo.embulk.input.union.plugin

import java.util.Optional

import org.embulk.config.{Config, ConfigDefault, ConfigSource, Task}
import org.embulk.spi.{ExecutorPlugin, OutputPlugin, Schema}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Using

object ReuseOutputLocalExecutorPlugin {
  trait PluginTask extends Task {
    @Config("max_threads")
    @ConfigDefault("null")
    def getMaxThreads: Optional[Int]
  }
}

case class ReuseOutputLocalExecutorPlugin(outputPlugin: OutputPlugin)
    extends ExecutorPlugin {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[ReuseOutputLocalExecutorPlugin])
  private lazy val numCores: Int = Runtime.getRuntime.availableProcessors

  override def transaction(
      config: ConfigSource,
      outputSchema: Schema,
      inputTaskCount: Int,
      control: ExecutorPlugin.Control
  ): Unit = {
    val task =
      config.loadConfig(classOf[ReuseOutputLocalExecutorPlugin.PluginTask])
    val maxThreads = task.getMaxThreads.orElse(numCores * 2)
    logger.info(
      s"Using local thread executor with max_threads=$maxThreads / tasks=$inputTaskCount"
    )
    Using.resource(
      new ReuseOutputLocalExecutor(outputPlugin, maxThreads, inputTaskCount)
    ) { exec: ReuseOutputLocalExecutor =>
      control.transaction(outputSchema, exec.getOutputTaskCount, exec)
    }

  }
}
