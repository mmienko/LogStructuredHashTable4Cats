package io.lsht

import cats.effect.*
import cats.effect.std.{Console, Queue, QueueSink, Supervisor}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monad, Monoid, Semigroup}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Chunk, Stream}
import io.lsht.LogStructuredHashTable.*
import io.lsht.codec.{DataFileDecoder, KeyValueCodec, TombstoneEncoder}

class LogStructuredHashTable[F[_]: Async] private[lsht] (
    queue: QueueSink[F, WriteCommand[F]],
    index: Ref[F, Map[Key, KeyValueFileReference]],
    isClosed: Ref[F, Boolean]
) {

  def get(key: Key): F[Option[Value]] =
    index.get.map(_.get(key)).flatMap {
      case Some(KeyValueFileReference(file, offset, length)) =>
        // TODO: Use an object pool for efficient resource/file management
        Files[F]
          .open(file, Flags.Read)
          .adaptErr { case err: java.nio.file.FileSystemException =>
            Errors.Read.FileSystem(err)
          }
          .use { fh =>
            for {
              bytes <- fh.read(numBytes = length, offset = offset)
              bytes <- ApplicativeError[F, Throwable]
                .fromOption(bytes, Errors.Read.CorruptedDataFile)
              _ <- ApplicativeError[F, Throwable]
                .raiseWhen(bytes.size != length)(
                  Errors.Read.CorruptedDataFile
                )
              putValue <- KeyValueCodec.decode(bytes)
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

  // TODO: Support
  def keys: Stream[F, Key] = Stream.empty

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
  TODO: Consider removing this
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
            .raiseError[Unit](Errors.Write.Failed(cause))
      }
    } yield ()
}

object LogStructuredHashTable {

  def apply[F[_]: Async: Console](directory: Path, limit: Int = 1000): Resource[F, LogStructuredHashTable[F]] =
    Database(directory, limit)

}
