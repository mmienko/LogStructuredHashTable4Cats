package io.lsht

import scala.util.control.NoStackTrace

object Errors {

  open class ReadException extends Throwable

  object Read {
    object BadChecksum extends ReadException with NoStackTrace

    object CorruptedDataFile extends ReadException with NoStackTrace
  }
}
