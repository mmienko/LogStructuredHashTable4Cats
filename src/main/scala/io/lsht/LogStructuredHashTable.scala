package io.lsht

import cats.effect.*
import cats.effect.std.{Console, QueueSink}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monoid, Semigroup}
import fs2.Stream
import fs2.io.file.{Files, Flags, Path}
import io.lsht.LogStructuredHashTable.*
import io.lsht.codec.{KeyValueCodec, ValuesCodec}

class LogStructuredHashTable[F[_]: Async] private[lsht] (
    queue: QueueSink[F, WriteCommand[F]],
    index: Ref[F, Map[Key, FileReference]],
    isClosed: Ref[F, Boolean]
) {

  def checkIntegrity(key: Key): F[Option[Throwable]] =
    for {
      res <- doGet(key).attempt
      res <- res match
        case Left(err: IllegalStateException) =>
          Async[F].raiseError(err)
        case _ =>
          res.swap.toOption.pure[F]
    } yield res

  def get(key: Key): F[Option[Value]] =
    for {
      res <- doGet(key).attempt
      res <- res match
        case Left(err: IllegalStateException) =>
          Async[F].raiseError(err)
        case _ =>
          res.toOption.flatten.pure[F]
    } yield res

  private def doGet(key: Key): F[Option[Value]] =
    index.get.map(_.get(key)).flatMap {
      case Some(fileReference) =>
        Files[F]
          .open(fileReference.file, Flags.Read)
          .adaptErr { case err: java.nio.file.FileSystemException =>
            ReadErrors.FileSystem(err)
          }
          .use { fh =>
            val length = fileReference.length
            for {
              bytes <- fh.read(numBytes = length, offset = fileReference.offset)
              bytes <- ApplicativeError[F, Throwable]
                .fromOption(bytes, ReadErrors.CorruptedDataFile)
              _ <- ApplicativeError[F, Throwable]
                .raiseWhen(bytes.size != length)(
                  ReadErrors.CorruptedDataFile
                )
              putValue <- (fileReference match
                case _: KeyValueFileReference =>
                  KeyValueCodec
                    .decode(bytes)
                case _: CompactedValueReference =>
                  ValuesCodec
                    .decode(bytes)
                    .map(KeyValue(key, _))
              ).adaptError(ReadErrors.FailedToDecode(_))
            } yield putValue.value.some
          }

      case None =>
        none[Value].pure[F]
    }

  def put(key: Key, value: Value): F[Unit] =
    submitWriteCommand(WriteCommand.Put(key, value))

  def delete(key: Key): F[Unit] =
    index.get
      .map(_.contains(key))
      .flatMap(Applicative[F].whenA(_)(submitWriteCommand(WriteCommand.Delete(key))))

  def keys: Stream[F, Key] =
    for {
      idx <- Stream.eval(index.get)
      key <- Stream.fromIterator(idx.keysIterator, chunkSize = 100)
    } yield key

  def entries[A]: Stream[F, (Key, Value)] =
    keys
      .evalMap(k => get(k).tupleLeft(k))
      .collect { case (k, Some(v)) => (k, v) }

  def fold[A](initial: A)(func: (A, (Key, Value)) => A): F[A] =
    entries
      .fold(initial)(func)
      .compile
      .last
      .map(_.getOrElse(initial))

  def foldMap[A: Semigroup](initial: A)(func: (Key, Value) => A): F[A] =
    fold(initial)((a, kv) => Semigroup[A].combine(a, func.tupled(kv)))

  def foldMap[A: Monoid](func: (Key, Value) => A): F[A] =
    foldMap(initial = Monoid[A].empty)(func)

  /*
  There is still a race whenever using this method, however it may help catch bugs during improper use of Resource's
   */
  private def validateDbIsOpen =
    isClosed.get.flatMap(
      ApplicativeError[F, Throwable].raiseWhen(_)(
        new IllegalStateException("Resource leak, db is closed and this method should not be called")
      )
    )

  private def submitWriteCommand[C <: WriteCommand[F]](makeWriteCommand: F[C]): F[Unit] =
    for {
      cmd <- makeWriteCommand
      _ <- queue.offer(cmd)
      _ <- validateDbIsOpen
      result <- cmd.waitUntilComplete
      _ <- result match {
        case () =>
          Applicative[F].unit
        case cause: Throwable =>
          ApplicativeError[F, Throwable]
            .raiseError[Unit](WriteErrors.Failed(cause))
      }
    } yield ()
}

object LogStructuredHashTable {

  def apply[F[_]: Async: Console](directory: Path, limit: Int = 1000): Resource[F, LogStructuredHashTable[F]] =
    Database(directory, limit)

  class ReadException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object ReadErrors {
    final case class FailedToDecode(cause: Throwable) extends ReadException(cause = cause.some)

    object CorruptedDataFile extends ReadException(cause = None)

    final case class FileSystem(cause: java.nio.file.FileSystemException) extends ReadException(cause.some)
  }

  class WriteException(cause: Option[Throwable]) extends Throwable(cause.orNull)

  object WriteErrors {
    class Failed(cause: Throwable) extends WriteException(cause.some)
  }

}
