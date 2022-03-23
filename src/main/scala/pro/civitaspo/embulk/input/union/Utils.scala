package pro.civitaspo.embulk.input.union

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import org.embulk.spi.PageOutput
import org.embulk.spi.PageBuilder

import scala.util.chaining.scalaUtilChainingOps

object Utils {
  import implicits._

  def genTransactionId(): String = {
    val uuid: UUID = UUID.randomUUID()
    val md: MessageDigest = MessageDigest.getInstance("SHA1")
    val sha1: String =
      md.digest(uuid.toString.getBytes(UTF_8)).map("%02x".format(_)).mkString
    sha1.take(7)
  }

  def recursiveExceptionMatch(ex: Throwable, klass: Class[_]): Option[_] = {
    Option(ex) match {
      case None                               => None
      case Some(ex1) if ex1.getClass == klass => Some(ex1)
      case Some(ex1) =>
        Option(ex1.getSuppressed) match {
          case Some(supp)
              if supp.exists(recursiveExceptionMatch(_, klass).isDefined) =>
            supp
              .map(recursiveExceptionMatch(_, klass))
              .find(_.isDefined)
              .flatten
          case _ =>
            Option(ex1.getCause) match {
              case Some(cause) => recursiveExceptionMatch(cause, klass)
              case _           => None
            }
        }
    }
  }

  private def isSamplingPageOutput(output: PageOutput): Boolean = {
    // https://github.com/embulk/embulk/blob/8c7b1ac02e8fff3902bff573fd1cdf1709d1cdaf/embulk-core/src/main/java/org/embulk/exec/PreviewExecutor.java#L144
    output.getClass() == Class.forName(
      "org.embulk.exec.PreviewExecutor$SamplingPageOutput"
    )
  }

  // CAUTION: This method extracts the SamplingPageOutput wrapped by the PageBuilders of the Filter
  // Plugins. The PageBuilders are searched and extracted, but the usage of PageBuilders depends on
  // the implementation of Filter Plugin, so it cannot be guaranteed to work properly.
  private def digSamplingPageOutput(output: PageOutput): PageOutput = {
    if (isSamplingPageOutput(output)) return output

    val fields = output.getClass.getDeclaredFields ++ output.getClass.getFields
    fields.find(f => classOf[PageBuilder].isAssignableFrom(f.getType)) match {
      case Some(f) =>
        val pb =
          f.tap(_.setAccessible(true)).get(output).asInstanceOf[PageBuilder]
        val o = classOf[PageBuilder]
          .getDeclaredField("output")
          .tap(_.setAccessible(true))
          .get(pb)
          .asInstanceOf[PageOutput]
        digSamplingPageOutput(o)

      case None =>
        throw new UnsupportedOperationException(
          s"Cannot find PageBuilder in ${output.getClass.getName}"
        )
    }
  }

  def shouldFinishSamplingPageOutput(output: PageOutput): Boolean = {
    val o = digSamplingPageOutput(output)
    // NOTE: Need to call `output.finish()` only once to avoid the error: org.embulk.exec.NoSampleException: No input records to preview
    // https://github.com/embulk/embulk/blob/8c7b1ac02e8fff3902bff573fd1cdf1709d1cdaf/embulk-core/src/main/java/org/embulk/exec/PreviewExecutor.java#L144-L194
    val sampleRows = o.getClass
      .getDeclaredField("sampleRows")
      .tap(_.setAccessible(true))
      .get(o)
      .asInstanceOf[Int]
    val recordCount = o.getClass
      .getDeclaredField("recordCount")
      .tap(_.setAccessible(true))
      .get(o)
      .asInstanceOf[Int]
    !(recordCount >= sampleRows)
  }
}
