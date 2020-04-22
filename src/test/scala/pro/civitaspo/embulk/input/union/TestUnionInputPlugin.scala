package pro.civitaspo.embulk.input.union

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
    with Diagrams {}
