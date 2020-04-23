package pro.civitaspo.embulk.input.union

import com.google.inject.{Binder, Guice, Module, Stage}
import org.embulk.{TestPluginSourceModule, TestUtilityModule}
import org.embulk.config.{
  ConfigLoader,
  ConfigSource,
  DataSourceImpl,
  ModelManager,
  TaskReport,
  TaskSource
}
import org.embulk.exec.{
  ExecModule,
  ExtensionServiceLoaderModule,
  SystemConfigModule
}
import org.embulk.jruby.JRubyScriptingModule
import org.embulk.plugin.{
  BuiltinPluginSourceModule,
  InjectedPluginSource,
  PluginClassLoaderModule
}
import org.embulk.spi.{Exec, ExecSession, InputPlugin, Page, Schema}
import org.embulk.spi.TestPageBuilderReader.MockPageOutput
import org.embulk.spi.util.Pages
import org.scalatest.BeforeAndAfter
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.ExecutionException
import scala.util.chaining._

object EmbulkTestHelper {
  case class TestRuntimeModule() extends Module {
    override def configure(binder: Binder): Unit = {
      val systemConfig = new DataSourceImpl(null)
      new SystemConfigModule(systemConfig).configure(binder)
      new ExecModule(systemConfig).configure(binder)
      new ExtensionServiceLoaderModule(systemConfig).configure(binder)
      new BuiltinPluginSourceModule().configure(binder)
      new JRubyScriptingModule(systemConfig).configure(binder)
      new PluginClassLoaderModule().configure(binder)
      new TestUtilityModule().configure(binder)
      new TestPluginSourceModule().configure(binder)
      InjectedPluginSource.registerPluginTo(
        binder,
        classOf[InputPlugin],
        "union",
        classOf[UnionInputPlugin]
      )
    }
  }

  def getExecSession: ExecSession = {
    val injector =
      Guice.createInjector(Stage.PRODUCTION, TestRuntimeModule())
    val execConfig = new DataSourceImpl(
      injector.getInstance(classOf[ModelManager])
    )
    ExecSession.builder(injector).fromExecConfig(execConfig).build()
  }
}

abstract class EmbulkTestHelper
    extends AnyFunSuite
    with BeforeAndAfter
    with Diagrams {

  import implicits._

  var exec: ExecSession = _

  before {
    exec = EmbulkTestHelper.getExecSession
  }
  after {
    exec.cleanup()
    exec = null
  }

  def runInput(
      inConfig: ConfigSource,
      test: Seq[Seq[AnyRef]] => Unit = { _ => }
  ): Unit = {
    try {
      Exec.doWith(
        exec,
        () => {
          exec.getInjector.getInstance(classOf[UnionInputPlugin]).tap {
            plugin =>
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
    new ConfigLoader(exec.getModelManager).fromYamlString(yaml)
  }

  def tsvResourceDir: String = {
    classOf[EmbulkTestHelper].getResource("/tsv").getPath
  }
}
