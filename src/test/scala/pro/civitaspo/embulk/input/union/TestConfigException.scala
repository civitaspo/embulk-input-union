package pro.civitaspo.embulk.input.union

import org.embulk.config.{ConfigException, ConfigSource}
import org.junit.runner.RunWith
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatestplus.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class TestConfigException
    extends AnyFunSuite
    with EmbulkTestHelper
    with BeforeAndAfter
    with Diagrams {

  test("Throw ConfigException when \"union\" option is undefined") {
    val yaml = "{}" // empty yaml
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    val caught = intercept[ConfigException](runInput(config))
    assert(caught.isInstanceOf[ConfigException])
    assert(
      caught.getMessage.contains("Field 'union' is required but not set")
    )
  }

  test("Throw ConfigException when the value of \"union\" option is empty") {
    val yaml = """
                 | union: []
                 |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    val caught = intercept[ConfigException](runInput(config))
    assert(caught.isInstanceOf[ConfigException])
    assert(
      caught.getMessage.startsWith("1 or more configurations are required.")
    )
  }

  test(
    "Throw ConfigException when different schema is found: Number of columns"
  ) {
    val yaml = """
                 | union:
                 |   - in:
                 |       type: config
                 |       columns:
                 |         - {name: c0, type: long}
                 |       values:
                 |         - - [1]
                 |           - [2]
                 |         - - [3]
                 |           - [4]
                 |   - in:
                 |       type: config
                 |       columns:
                 |         - {name: c0, type: long}
                 |         - {name: c1, type: long}
                 |       values:
                 |         - - [1, 1]
                 |           - [2, 2]
                 |         - - [3, 3]
                 |           - [4, 4]
                 |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    val caught = intercept[ConfigException](runInput(config))
    assert(caught.isInstanceOf[ConfigException])
    assert(caught.getMessage.startsWith("Different schema is found:"))
  }

  test(
    "Throw ConfigException when different schema is found: Type of columns"
  ) {
    val yaml = """
                 | union:
                 |   - in:
                 |       type: config
                 |       columns:
                 |         - {name: c0, type: long}
                 |       values:
                 |         - - [1]
                 |           - [2]
                 |         - - [3]
                 |           - [4]
                 |   - in:
                 |       type: config
                 |       columns:
                 |         - {name: c0, type: string}
                 |         - {name: c1, type: long}
                 |       values:
                 |         - - [a, 1]
                 |           - [b, 2]
                 |         - - [c, 3]
                 |           - [d, 4]
                 |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    val caught = intercept[ConfigException](runInput(config))
    assert(caught.isInstanceOf[ConfigException])
    assert(caught.getMessage.startsWith("Different schema is found:"))
  }
}
