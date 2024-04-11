package io.lsht

import cats.syntax.option.*

import scala.util.control.NoStackTrace

object Errors {

  class StartupException extends Throwable

  object Startup {
    object PathNotADirectory extends StartupException with NoStackTrace
    object BadChecksum extends StartupException with NoStackTrace
    object MissingTombstoneKey extends StartupException with NoStackTrace
    object MissingValueSize extends StartupException with NoStackTrace
    object MissingKeyValueEntry extends StartupException with NoStackTrace
  }

  class ReadException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object Read {
    object BadChecksum extends ReadException(cause = None) with NoStackTrace

    object CorruptedDataFile extends ReadException(cause = None) with NoStackTrace

    final case class FileSystem(cause: java.nio.file.FileSystemException)
        extends ReadException(cause.some)
        with NoStackTrace
  }

  class WriteException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object Write {
    class Failed(cause: Throwable) extends WriteException(cause.some) with NoStackTrace
    object Cancelled extends WriteException(cause = None) with NoStackTrace
    object CancelledButSavedToDisk extends WriteException(cause = None) with NoStackTrace
  }
}
