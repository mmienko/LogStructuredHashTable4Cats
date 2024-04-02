package io.lsht

import scala.util.control.NoStackTrace

object Errors {

  open class StartupException extends Throwable

  object Startup {
    object PathNotADirectory extends StartupException with NoStackTrace
  }

  open class ReadException extends Throwable

  object Read {
    object BadChecksum extends ReadException with NoStackTrace

    object CorruptedDataFile extends ReadException with NoStackTrace
  }

  open class WriteException(cause: Throwable) extends Throwable(cause)

  object Write {
    class Failed(cause: Throwable) extends WriteException(cause) with NoStackTrace
  }
}
