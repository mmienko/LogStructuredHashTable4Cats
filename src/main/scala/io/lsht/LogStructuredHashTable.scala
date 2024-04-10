package io.lsht

import cats.effect.*
import cats.effect.std.{Console, Queue, QueueSink, Supervisor}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monad, Monoid, Semigroup}
import fs2.io.file.{Files, Flags, Path}
import fs2.{Chunk, Stream}
import io.lsht.LogStructuredHashTable.*

class LogStructuredHashTable[F[_]: Async] private (
    queue: QueueSink[F, PutCommand[F]],
    index: Ref[F, Map[Key, EntryFileReference]],
    isClosed: Ref[F, Boolean]
) {

  def get(key: Key): F[Option[Value]] =
    index.get.map(_.get(key)).flatMap {
      case Some(EntryFileReference(filePath, positionInFile, entrySize)) =>
        // TODO: Use an object pool for efficient resource/file management
        Files[F]
          .open(filePath, Flags.Read)
          .adaptErr { case err: java.nio.file.FileSystemException =>
            Errors.Read.FileSystem(err)
          }
          .use { fh =>
            for {
              bytes <- fh.read(numBytes = entrySize, offset = positionInFile)
              bytes <- ApplicativeError[F, Throwable]
                .fromOption(bytes, Errors.Read.CorruptedDataFile)
              _ <- ApplicativeError[F, Throwable]
                .raiseWhen(bytes.size != entrySize)(
                  Errors.Read.CorruptedDataFile
                )
              putValue <- KeyValueEntryCodec.decode(bytes)
            } yield putValue.value.some
          }

      case None =>
        none[Value].pure[F]
    }

  def put(key: Key, value: Value): F[Unit] =
    for {
      cmd <- PutCommand(key, value)
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

}

object LogStructuredHashTable {

  // TODO: s/Console/Logger
  def apply[F[_]: Async: Console](
      directory: Path
  ): Resource[F, LogStructuredHashTable[F]] = {
    Resource.suspend {
      for {
        _ <- verifyPathIsDirectory[F](directory)

        files <- getFiles(directory)
      } yield files.sortBy(_.fileName.toString).toList match {
        case writerFile :: otherFiles =>
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
          // TODO: for big files, can we create pointers to different points to read in parallel, this would speedup
          //  start times. So first few bytes of file would contain a pointer to another spot in file, only needed
          //  for large files. There is a readRage
          // TODO: Resource.force/suspend ?
          Resource.suspend {
            for {
              _ <- ApplicativeError[F, Throwable].raiseUnless(otherFiles.isEmpty)(
                new UnsupportedOperationException("Assuming no file rotation")
              )
              index <- Ref[F].of(Map.empty[Key, EntryFileReference])
              _ <- Files[F]
                .readAll(writerFile)
                .through(KeyValueEntryCodec.decode[F])
                .evalMap {
                  case (Left(err), offset) =>
                    Console[F].println(err.toString + s" at offset $offset")

                  case (Right(entry), offset) =>
                    index.update(
                      _.updated(
                        entry.key,
                        EntryFileReference(
                          writerFile,
                          positionInFile = offset,
                          entrySize = KeyValueEntryCodec.HeaderSize + entry.size
                        )
                      )
                    )
                }
                .compile
                .drain
            } yield runDatabase(writerFile, index)
          }

        case Nil =>
          // new DB
          Resource.suspend {
            for {
              now <- Clock[F].realTime

              writerFile = directory / s"data.${now.toMillis.toString}.db"

              // TODO: what happens if file already exists? B/c two programs are running?
              _ <- Files[F].createFile(writerFile)

              index <- Ref[F].of(Map.empty[Key, EntryFileReference])
            } yield runDatabase(writerFile, index)
          }
      }
    }
  }

  private def runDatabase[F[_]: Async: Console: Clock](writerFile: Path, index: Ref[F, Map[Key, EntryFileReference]]) =
    for {
      queue <- Resource.eval(Queue.unbounded[F, PutCommand[F]])

      cancelRemainingCommands = drain(queue).flatMap(_.traverse_(_.complete(Errors.Write.Cancelled)))
      seriallyExecuteWrites = MonadCancel[F].guaranteeCase(
        Stream
          .fromQueueUnterminated(queue, limit = 1) // TODO: what should limit be?
          .evalMap(executeCommand(writerFile, index))
          .compile
          .drain
      ) {
        case Outcome.Succeeded(_) =>
          ().pure[F]

        case Outcome.Errored(e) =>
          drain(queue).flatMap(_.traverse_(_.complete(e)))

        case Outcome.Canceled() =>
          cancelRemainingCommands
      }

      isClosed <- Resource.eval(Ref[F].of(false))

      _ <- Supervisor[F](await = false)
        .evalMap(_.supervise(seriallyExecuteWrites))
        .onFinalize(isClosed.set(true) >> cancelRemainingCommands)
    } yield new LogStructuredHashTable(queue, index, isClosed)

  private def verifyPathIsDirectory[F[_]: Async](directory: Path) =
    Files[F]
      .isDirectory(directory, followLinks = false)
      .flatMap {
        ApplicativeError[F, Throwable]
          .raiseUnless(_)(Errors.Startup.PathNotADirectory)
      }

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

  private def executeCommand[F[_]: Async: Console: Clock](
      writerFile: Path,
      index: Ref[F, Map[Key, EntryFileReference]]
  )(putCmd: PutCommand[F]) = {
    def guarantee[A](fa: F[A])(onCancel: => Errors.WriteException): F[A] =
      MonadCancel[F].guaranteeCase(fa) {
        case Outcome.Succeeded(_) => ().pure[F]
        case Outcome.Errored(e) => putCmd.complete(Errors.Write.Failed(e)).void
        case Outcome.Canceled() => putCmd.complete(onCancel).void
      }

    for {
      // Encode Key-Value Pair
      bytes <- guarantee(KeyValueEntryCodec.encode(putCmd.keyValueEntry))(
        onCancel = Errors.Write.Cancelled
      )

      // Write to file, rotating if necessary
      // TODO: Copy Files.writeRotate but operate at the ByteBuffer level. Rotation could simply be on number
      //  of entries, which will reduce startup times to load index. Or a secondary threshold that measures
      //  the number of rewrites to keys, the more rewrites, the more the file can be compacted, thus saving
      //  space.
      positionOfEntry <- guarantee(
        Files[F].open(writerFile, Flags.Append).use { fh =>
          fh.size
            .flatMap { offset =>
              fh.write(Chunk.byteBuffer(bytes), offset)
                .as(offset)
            }
        }
      )(onCancel = Errors.Write.CancelledButSavedToDisk)

      // Update in-memory index, for reads
      _ <- guarantee(
        index.update(
          _.updated(
            putCmd.key,
            EntryFileReference(
              filePath = writerFile,
              positionInFile = positionOfEntry,
              entrySize = bytes.capacity()
            )
          )
        )
      )(onCancel = Errors.Write.CancelledButSavedToDisk)

      // Signal complete to writer thread
      _ <- putCmd.complete(())
    } yield ()
  }

  private def drain[F[_]: Monad](
      queue: Queue[F, PutCommand[F]]
  ): F[List[PutCommand[F]]] =
    queue.size
      .map(_ + 100)
      .map(_.some)
      .flatMap(queue.tryTakeN)

  private final case class EntryFileReference(filePath: Path, positionInFile: Long, entrySize: Int)

}
