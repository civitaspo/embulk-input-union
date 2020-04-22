package pro.civitaspo.embulk.input.union

import org.embulk.config.{ConfigLoader, ConfigSource, TaskReport, TaskSource}
import org.embulk.EmbulkTestRuntime
import org.embulk.spi.{Exec, Page, Schema}
import org.embulk.spi.TestPageBuilderReader.MockPageOutput
import org.embulk.spi.util.Pages
import org.junit.Rule

import scala.annotation.meta.getter
import scala.concurrent.ExecutionException
import scala.util.chaining._

trait EmbulkTestHelper {
  import implicits._

  @(Rule @getter)
  def runtime: EmbulkTestRuntime = new EmbulkTestRuntime()

  def runInput(
      inConfig: ConfigSource,
      test: Seq[Seq[AnyRef]] => Unit = { _ => }
  ): Unit = {
    try {
      Exec.doWith(
        runtime.getExec,
        () => {
          runtime.getInstance(classOf[UnionInputPlugin]).tap { plugin =>
            plugin.transaction(
              inConfig,
              (taskSource: TaskSource, schema: Schema, taskCount: Int) => {
                val outputs: Seq[MockPageOutput] =
                  0.until(taskCount).map { _ => new MockPageOutput() }
                val reports: Seq[TaskReport] = 0.until(taskCount).map {
                  taskIndex =>
                    plugin
                      .run(taskSource, schema, taskIndex, outputs(taskIndex))
                }
                val pages: Seq[Page] = outputs.flatMap(_.pages)
                test(Pages.toObjects(schema, pages).map(_.toSeq))

                reports
              }
            )
          }
        }
      )
    }
    catch {
      case ex: ExecutionException => throw ex.getCause
    }
  }

  def loadConfigSourceFromYamlString(yaml: String): ConfigSource = {
    new ConfigLoader(runtime.getModelManager).fromYamlString(yaml)
  }
}
