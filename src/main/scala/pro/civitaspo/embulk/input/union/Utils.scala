package pro.civitaspo.embulk.input.union

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID
import com.google.common.cache.CacheBuilder
import java.util.concurrent.TimeUnit
import org.embulk.spi.PageOutput

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

  def isSamplingPageOutput(output: PageOutput): Boolean = {
    // https://github.com/embulk/embulk/blob/8c7b1ac02e8fff3902bff573fd1cdf1709d1cdaf/embulk-core/src/main/java/org/embulk/exec/PreviewExecutor.java#L144
    val klass =
      Class.forName("org.embulk.exec.PreviewExecutor$SamplingPageOutput")
    output.getClass() == klass
  }

  def shouldFinishSamplingPageOutput(output: PageOutput): Boolean = {
    if (!isSamplingPageOutput(output))
      throw new IllegalArgumentException(
        s"output is not sampling page output.: ${output.getClass.getName}"
      )
    // NOTE: Need to call `output.finish()` only once to avoid the error: org.embulk.exec.NoSampleException: No input records to preview
    // https://github.com/embulk/embulk/blob/8c7b1ac02e8fff3902bff573fd1cdf1709d1cdaf/embulk-core/src/main/java/org/embulk/exec/PreviewExecutor.java#L144-L194
    val sampleRows = output.getClass.getDeclaredField("sampleRows")
    sampleRows.setAccessible(true)
    val recordCount = output.getClass.getDeclaredField("recordCount")
    recordCount.setAccessible(true)
    !(recordCount.get(output).asInstanceOf[Int] >= sampleRows
      .get(output)
      .asInstanceOf[Int])
  }
}
