package pro.civitaspo.embulk.input.union

import pro.civitaspo.embulk.input.union.ThreadNameContext.trimThreadId

object ThreadNameContext {
  def switch[A](name: String)(f: ThreadNameContext => A): A = {
    ThreadNameContext(current = currentThreadName, next = name)
      .switch(f)
  }

  private def trimThreadId(str: String): String =
    str.replaceFirst("^\\d{4}:", "")
  private def currentThreadName: String = Thread.currentThread.getName

  def isPreviewExecution: Boolean = currentThreadName.contains("preview")
}

case class ThreadNameContext private (
    private val current: String,
    private val next: String
) {
  private lazy val currentWithoutThreadId = trimThreadId(current)
  private lazy val nextWithoutThreadId = trimThreadId(next)

  def switch[A](f: ThreadNameContext => A): A = {
    val currentThread = Thread.currentThread()
    val currentThreadId: String = String.format("%04d", currentThread.getId)
    try {
      currentThread.setName(s"$currentThreadId:$nextWithoutThreadId")
      f(copy(current = next, next = current))
    }
    finally currentThread.setName(s"$currentThreadId:$currentWithoutThreadId")
  }

  def switch[A](name: String)(f: ThreadNameContext => A): A =
    copy(next = name).switch(f)
}
