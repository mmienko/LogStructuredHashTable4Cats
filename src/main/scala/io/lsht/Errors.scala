package io.lsht

import scala.concurrent.CancellationException
import scala.util.control.NoStackTrace

object Errors {

  class StartupException extends Throwable

  object Startup {
    object PathNotADirectory extends StartupException with NoStackTrace
  }

  class ReadException extends Throwable

  object Read {
    object BadChecksum extends ReadException with NoStackTrace

    object CorruptedDataFile extends ReadException with NoStackTrace
  }

  class WriteException(cause: Throwable) extends Throwable(cause)

  object Write {
    class Failed(cause: Throwable) extends WriteException(cause) with NoStackTrace
    object Cancelled extends WriteException(new CancellationException with NoStackTrace) with NoStackTrace
    object CancelledButSavedToDisk extends WriteException(new CancellationException with NoStackTrace) with NoStackTrace
  }
}
