package pro.civitaspo.embulk.input.union

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID

object Utils {
  def genTransactionId(): String = {
    val uuid: UUID = UUID.randomUUID()
    val md: MessageDigest = MessageDigest.getInstance("SHA1")
    val sha1: String =
      md.digest(uuid.toString.getBytes(UTF_8)).map("%02x".format(_)).mkString
    sha1.take(7)
  }
}
