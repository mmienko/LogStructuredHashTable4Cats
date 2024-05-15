package io.lsht

import cats.syntax.option.*
import fs2.io.file.Path

import scala.util.control.NoStackTrace

object Errors {

  class StartupException extends Throwable

  object Startup {
    object PathNotADirectory extends StartupException
    object BadChecksum extends StartupException
    object MissingTombstoneKey extends StartupException
    object MissingValueSize extends StartupException
    object MissingKeyValueEntry extends StartupException
  }

  class ReadException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object Read {
    object BadChecksum extends ReadException(cause = None)

    object CorruptedDataFile extends ReadException(cause = None)

    final case class FileSystem(cause: java.nio.file.FileSystemException) extends ReadException(cause.some)
  }

  class WriteException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object Write {
    class Failed(cause: Throwable) extends WriteException(cause.some)
    object Cancelled extends WriteException(cause = None)
    object CancelledButSavedToDisk extends WriteException(cause = None)
  }

  class CompactionException(message: String) extends Throwable(message)

  object CompactionException {
    class SeekAndReadFailedOnDataFile(file: Path, offset: Offset)
        extends CompactionException(s"Could not read Corrupted DataFile ${file.toString} at offset ${offset.toString}")
  }
}
