package pro.civitaspo.embulk.input.union

import org.embulk.config.ConfigSource
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.diagrams.Diagrams
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestUnionInputPlugin
    extends AnyFunSuite
    with EmbulkTestHelper
    with BeforeAndAfter
    with Diagrams {

  test("Can union 3 sources") {
    val yaml =
      """
        | union:
        |   - in:
        |       type: config
        |       columns:
        |         - {name: c0, type: long}
        |       values:
        |         - - [0]
        |   - in:
        |       type: config
        |       columns:
        |         - {name: c0, type: long}
        |       values:
        |         - - [1]
        |   - in:
        |       type: config
        |       columns:
        |         - {name: c0, type: long}
        |       values:
        |         - - [2]
        |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    runInput(
      config, { result: Seq[Seq[AnyRef]] =>
        assert(result.size == 3, "The size of records is 3.")

        assert(
          result.head.head.asInstanceOf[Long] == 0,
          "The value of the first record is 0."
        )
        assert(
          result(1).head.asInstanceOf[Long] == 1,
          "The value of the second record is 1."
        )
        assert(
          result(2).head.asInstanceOf[Long] == 2,
          "The value of the third record is 2."
        )
      }
    )
  }

  test("Can use the plugin that extends FileInputPlugin.") {
    val yaml =
      s"""
         | union:
         |   - in:
         |       type: file
         |       path_prefix: ${tsvResourceDir}/data
         |       parser:
         |         type: csv
         |         delimiter: "\t"
         |         skip_header_lines: 0
         |         null_string: ""
         |         columns:
         |           - { name: id,      type: long }
         |           - { name: name,    type: string }
         |           - { name: src,     type: string }
         |           - { name: t,       type: timestamp, format: "%Y-%m-%d %H:%M:%S %z" }
         |           - { name: payload, type: json }
         |         stop_on_invalid_record: true
         |   - in:
         |       type: config
         |       columns:
         |         - { name: id,      type: long }
         |         - { name: name,    type: string }
         |         - { name: src,     type: string }
         |         - { name: t,       type: timestamp, format: "%Y-%m-%d %H:%M:%S %z" }
         |         - { name: payload, type: json }
         |       values:
         |         - - [ 0, c20ef94602, config, "2017-10-24 03:54:35 +0900", { a: 0, b: 99 } ]
         |           - [ 1, 330a9fc33a, config, "2017-10-22 19:53:31 +0900", { a: 1, b: a9 } ]
         |         - - [ 2, 707b3b7588, config, "2017-10-23 23:42:43 +0900", { a: 2, b: 96 } ]
         |           - [ 3,       null, config, "2017-10-22 06:12:13 +0900", { a: 3, b: 86 } ]
         |         - - [ 4, c54d8b6481, config, "2017-10-23 04:59:16 +0900", { a: 4, b: d2 } ]
         |
         |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    runInput(
      config, { result: Seq[Seq[AnyRef]] =>
        assert(result.size == 30, "The size of records is 30.")

        assert(
          result.take(25).map(_(2).asInstanceOf[String]).forall(_ == "file"),
          "The first 25 records are sourced from \"file\"."
        )
        assert(
          result
            .takeRight(5)
            .map(_(2).asInstanceOf[String])
            .forall(_ == "config"),
          "The last 5 records are sourced from \"config\"."
        )
      }
    )
  }

  test("Can use filters") {
    val yaml =
      """
        | union:
        |   - in:
        |       type: config
        |       columns:
        |         - {name: c0, type: long}
        |       values:
        |         - - [0]
        |     filters:
        |       - type: rename
        |         columns:
        |           c0: c1
        |   - in:
        |       type: config
        |       columns:
        |         - {name: c1, type: long}
        |       values:
        |         - - [1]
        |""".stripMargin
    val config: ConfigSource = loadConfigSourceFromYamlString(yaml)
    runInput(
      config, { result: Seq[Seq[AnyRef]] =>
        assert(result.size == 2, "The size of records is 2.")

        assert(
          result.head.head.asInstanceOf[Long] == 0,
          "The value of the first record is 0."
        )
        assert(
          result(1).head.asInstanceOf[Long] == 1,
          "The value of the second record is 1."
        )
      }
    )
  }
}
