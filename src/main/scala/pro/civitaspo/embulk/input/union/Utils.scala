package pro.civitaspo.embulk.input.union

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID

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
}
