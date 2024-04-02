package io.lsht

import cats.effect.std.{Console, Queue, QueueSink, Supervisor}
import cats.effect.*
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monoid, Semigroup}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Chunk, Stream}
import io.lsht.LogStructuredHashTable.*

import java.nio.ByteBuffer
import scala.util.control.NoStackTrace

class LogStructuredHashTable[F[_]: Async] private (
  queue: QueueSink[F, Put[F]],
  index: Ref[F, Map[Key, EntryFileReference]]
) {

  def get(key: Key): F[Option[Value]] =
    index.get.map(_.get(key)).flatMap {
      case Some(EntryFileReference(filePath, positionInFile, entrySize)) =>
        // TODO: Use an object pool for efficient resource/file management
        Files[F].open(filePath, Flags.Read).use { fh =>
          for {
            bytes <- fh.read(numBytes = entrySize, offset = positionInFile)
            bytes <- ApplicativeError[F, Throwable]
              .fromOption(bytes, Errors.Read.CorruptedDataFile)
            putValue <- PutCodec.decode(bytes)
          } yield putValue.value.some
        }

      case None =>
        none[Value].pure[F]
    }

  def put(key: Key, value: Value): F[Unit] =
    for {
      signal <- Deferred[F, PutResult]
      _ <- queue.offer(Put(key, value, signal))
      putResult <- signal.get
      _ <- putResult match {
        case () =>
          Applicative[F].unit
        case cause: Throwable =>
          ApplicativeError[F, Throwable].raiseError[Unit](WriteFailure(cause))
      }
    } yield ()

  // TODO: Support
  def delete(key: Key): F[Unit] =
    ApplicativeError[F, Throwable].raiseError(
      new UnsupportedOperationException("Delete not yet supported")
    )

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
}

object LogStructuredHashTable {

  // TODO: s/Console/Logger
  def apply[F[_]: Async: Console](
    directory: Path
  ): Resource[F, LogStructuredHashTable[F]] =
    for {
      _ <- Resource.eval(verifyPathIsDirectory[F](directory))

      files <- Resource.eval(getFiles(directory))

      db <- files.sortBy(_.fileName.toString).toList match {
        case ::(head, next) =>
          /*
          Seems easiest, from rotating pov to have "active writer file" and "older data files" have same name, but use
          a timestamp to differentiate between the two. As a matter of fact, on each new startup, simply create a new
          "active writer file", and let the previous "active writer file" be an "older data files". Compaction will
          handle cleaning up the files.

          The database is opened after a clean close. Therefore,
            -- there is an active writer file
            -- there *may* be older data files
            -- there *may* be merged data files w/ corresponding hint files
          If there are merged data files and older data files, then older data files should take precedence
          as they could contain the latest values for keys. So precedence order is
          "active file" > "older data file" > "merged data file".

          The database is opened after a crash close. Therefore,
            -- compaction is incomplete
              -- hint file was created but not corresponding merged values files, or vice versa.
              -- "merged data file" was created but "older data files" were not deleted.
              -- This could be solved with Write-Ahead-Intent-Log to disambiguate?
            -- a write is incomplete
              -- N/A CRC will check, then it should be marked for deletion to clean upon merge.
            -- an active writer file rotation is incomplete
              -- This could be solved with Alternating-Bit (or timestamp) to discern which file is latest. Or even
                 by file size.
            -- still empty as it closed after being created
          and a repair process is needed.

          TODO: Can this be made into a TLA+ spec?
           */
          // TODO: Support
          createNewDatabase(directory)

        case Nil =>
          // new DB
          createNewDatabase(directory)
      }
    } yield db

  private def createNewDatabase[F[_]: Async: Console: Clock](directory: Path) =
    for {
      now <- Resource.eval(Clock[F].realTime)

      writerFile = directory / s"data.${now.toMillis.toString}.db"

      // TODO: what happens if file already exists? B/c two programs are running?
      _ <- Resource.eval(Files[F].createFile(writerFile))

      // TODO: how does Array[Byte] hash?
      index <- Resource.eval(Ref[F].of(Map.empty[Key, EntryFileReference]))

      queue <- Resource.eval(Queue.unbounded[F, Put[F]])

      // TODO: Since cancellation is allowed, use bracket to handle writing to file (bundle all the evalMap's) and
      //  bracket the whole stream itself. Each calls complete on Write's, latter drains the whole queue. Should there
      //  also be a flag to prevent adding to queue? Probably not b/c only inner resources would write and they would
      //  be closed first, unless there is a leak or some background task.
      supervisor <- Supervisor[F](await = false)

      handleWrites = Stream
        .fromQueueUnterminated(queue, limit = 1) // TODO: what should limit be?
        .evalMap { put =>
          // TODO: onError: complete with error
          for {
            // Encode Key-Value Pair
            bytes <- PutCodec.encode(put)

            // Write to file, rotating if necessary
            // TODO: Copy Files.writeRotate but operate at the ByteBuffer level. Rotation could simply be on number
            //  of entries, which will reduce startup times to load index. Or a secondary threshold that measures
            //  the number of rewrites to keys, the more rewrites, the more the file can be compacted, thus saving
            //  space.
            positionOfEntry <- Files[F].open(writerFile, Flags.Append).use {
              fh =>
                fh.size
                  .flatMap { offset =>
                    fh.write(Chunk.byteBuffer(bytes), offset)
                      .as(offset)
                  }
            }

            // Update in-memory index, for reads
            _ <- index
              .update(
                _.updated(
                  put.key,
                  EntryFileReference(
                    filePath = writerFile, // TODO: Need to pass writerFile from potential rotation
                    positionInFile = positionOfEntry,
                    entrySize = bytes.capacity()
                  )
                )
              )

            // Signal complete to writer thread
            _ <- put.signal.complete(())
          } yield ()
        }
        .compile
        .drain

      _ <- Resource.eval(supervisor.supervise(handleWrites))
    } yield new LogStructuredHashTable(queue, index)

  private def verifyPathIsDirectory[F[_]: Async](directory: Path) =
    Files[F]
      .isDirectory(directory, followLinks = false)
      .flatMap(
        ApplicativeError[F, Throwable].raiseUnless(_)(PathNotADirectory())
      )

  private def getFiles[F[_]: Async: Console](directory: Path): F[Vector[Path]] =
    Files[F]
      .list(directory)
      .evalFilter { path =>
        Files[F]
          .isRegularFile(path)
          .flatTap { isRegularFile =>
            Applicative[F].unlessA(isRegularFile)(
              Console[F].println(s"Unknown path: ${path.toString}")
            )
          }
      }
      .compile
      .toVector

  private final case class EntryFileReference(filePath: Path,
                                              positionInFile: Long,
                                              entrySize: Int)

  final class PathNotADirectory extends Throwable with NoStackTrace

  final class WriteFailure(cause: Throwable)
      extends Throwable(cause)
      with NoStackTrace

}
