package io.lsht

import cats.data.NonEmptyList
import cats.effect.*
import cats.effect.std.{Console, Hotswap, Queue, Supervisor}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monad}
import fs2.io.file.*
import fs2.{Chunk, Pipe, Pull, Stream}
import io.lsht.LogStructuredHashTable.*
import io.lsht.codec.{DataFileDecoder, KeyValueCodec, TombstoneEncoder}

// TODO: just use database instead of HashTable object
object Database {

  // TODO: s/Console/Logger
  def apply[F[_]: Async: Console](directory: Path, entriesLimit: Int = 1000): Resource[F, LogStructuredHashTable[F]] =
    Resource.suspend {
      for {
        _ <- verifyPathIsDirectory[F](directory)

        files <- getFiles(directory)
      } yield NonEmptyList.fromFoldable(files.sortBy(_.fileName.toString)) match
        case Some(dataFiles) =>
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
          Resource.suspend {
            for {
              _ <- ApplicativeError[F, Throwable].raiseUnless(
                dataFiles.forall(_.fileName.toString.startsWith("data."))
              )(new UnsupportedOperationException("Assuming only rotated data files"))

              index <- Ref[F].of(Map.empty[Key, KeyValueFileReference])

              // TODO: fold with map, rather then ref
              counts <- dataFiles.traverse { dataFile =>
                Files[F]
                  .readAll(dataFile)
                  .through(DataFileDecoder.decode[F])
                  .evalMap {
                    case (Left(err), offset) =>
                      // whether kv or tombstone should be in error
                      Console[F].println(err.toString + s" at offset $offset")

                    case (Right(kv: KeyValue), offset) =>
                      index.update(
                        _.updated(
                          kv.key,
                          KeyValueFileReference(
                            dataFile,
                            offset = offset,
                            length = KeyValueCodec.size(kv)
                          )
                        )
                      )

                    case (Right(key: DataFileDecoder.Tombstone), _) =>
                      index.update(_.removed(key))
                  }
                  .fold(0) { case (count, _) => count + 1 }
                  .compile
                  .last
              }

              count = counts.last
            } yield runDatabase(
              directory,
              initialActiveDataFile = dataFiles.last,
              initialEntries = count.getOrElse(0),
              index,
              entriesLimit
            )
          }

        case None =>
          // new DB
          Resource.suspend {
            for {
              activeDataFile <- newDataFileName(directory)

              // TODO: what happens if file already exists? B/c two programs are running?
              _ <- Files[F].createFile(activeDataFile)

              index <- Ref[F].of(Map.empty[Key, KeyValueFileReference])
            } yield runDatabase(directory, activeDataFile, initialEntries = 0, index, entriesLimit)
          }
    }

  private def runDatabase[F[_]: Async: Console: Clock](
      dbDirectory: Path,
      initialActiveDataFile: Path,
      initialEntries: Int,
      index: Ref[F, Map[Key, KeyValueFileReference]],
      entriesLimit: Int
  ) =
    for {
      queue <- Resource.eval(Queue.unbounded[F, WriteCommand[F]])

      cancelRemainingCommands = drain(queue).flatMap(_.traverse_(_.complete(Errors.Write.Cancelled)))
      // TODO: for cancellation, maybe have queue be None terminated, then on close send None?
      seriallyExecuteWrites = MonadCancel[F].guaranteeCase(
        Stream
          .fromQueueUnterminated(queue, limit = 1) // TODO: what should limit be? Higher is better for performancea
          .evalMap(interpretCommand(index))
          .flattenOption
          .through(executeWriteAndRotateFile(dbDirectory, initialActiveDataFile, initialEntries, entriesLimit))
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

  private def newDataFileName[F[_]: Clock: Applicative](directory: Path) =
    Clock[F].realTime.map(now => directory / s"data.${now.toMillis.toString}.db")

  private def interpretCommand[F[_]: Async: Console: Clock](index: Ref[F, Map[Key, KeyValueFileReference]])(
      cmd: WriteCommand[F]
  ): F[Option[BytesToFile[F]]] =
    cmd match // tODO: guaranteeCommandCompletes
      case WriteCommand.Put(kv, signal) =>
        KeyValueCodec
          .encode(kv)
          .map(bytes =>
            BytesToFile(
              bytes,
              onWrite = (offset, dataFile) =>
                index.update(
                  _.updated(
                    kv.key,
                    KeyValueFileReference(
                      file = dataFile,
                      offset = offset,
                      length = bytes.capacity()
                    )
                  )
                ) *> signal.complete(()).void
            ).some
          )

      case WriteCommand.Delete(key, signal) =>
        index.get
          .map(_.contains(key))
          .flatMap {
            case false =>
              signal.complete(()).as(none[BytesToFile[F]])

            case true =>
              TombstoneEncoder
                .encode(key)
                .map(bytes =>
                  BytesToFile(
                    bytes,
                    onWrite = (_, _) => index.update(_.removed(key)) *> signal.complete(()).void
                  ).some
                )
          }

  private def drain[F[_]: Monad](
      queue: Queue[F, WriteCommand[F]]
  ): F[List[WriteCommand[F]]] =
    queue.size
      .map(_ + 100)
      .map(_.some)
      .flatMap(queue.tryTakeN)

  private def guaranteeCommandCompletes[F[_]: Sync, A](
      writeCmd: WriteCommand[F]
  )(fa: F[A], onCancel: => Errors.WriteException): F[A] =
    MonadCancel[F].guaranteeCase(fa) {
      case Outcome.Succeeded(_) => ().pure[F]
      case Outcome.Errored(e) => writeCmd.complete(Errors.Write.Failed(e)).void
      case Outcome.Canceled() => writeCmd.complete(onCancel).void
    }

  private def guaranteeCommandCompletes[F[_]: Sync, A](
      signal: Deferred[F, WriteResult]
  )(fa: F[A], onCancel: => Errors.WriteException): F[A] =
    MonadCancel[F].guaranteeCase(fa) {
      case Outcome.Succeeded(_) => ().pure[F]
      case Outcome.Errored(e) => signal.complete(Errors.Write.Failed(e)).void
      case Outcome.Canceled() => signal.complete(onCancel).void
    }

  // TODO: create new file each time, ignore an initial file. Rotation could simply be on number
  //  of entries, which will reduce startup times to load index. Or a secondary threshold that measures
  //  the number of rewrites to keys, the more rewrites, the more the file can be compacted, thus saving
  //  space.
  private[lsht] def executeWriteAndRotateFile[F[_]: Async](
      dbDirectory: Path,
      initialFile: Path,
      initialEntries: Int,
      entriesLimit: Int
  ): Pipe[F, BytesToFile[F], Unit] = {
    def toCursor(file: FileHandle[F]): F[WriteCursor[F]] =
      Files[F].writeCursorFromFileHandle(file, append = true)

    def swapFile(fileHotswap: Hotswap[F, FileHandle[F]]) =
      newDataFileName(dbDirectory).flatMap { path =>
        fileHotswap
          .swap(Files[F].open(path, Flags.Append))
          .flatMap(toCursor)
          .tupleRight(path)
      }

    def go(
        s: Stream[F, BytesToFile[F]],
        fileHotswap: Hotswap[F, FileHandle[F]],
        currentFile: Path,
        cursor: WriteCursor[F],
        entriesWritten: Int
    ): Pull[F, Unit, Unit] = {
      s.pull.uncons1.flatMap {
        case Some((bytesToFile, tail)) =>
          for {
            updatedCursor <- cursor.writePull(Chunk.byteBuffer(bytesToFile.bytebuffer))
            _ <- Pull.eval(bytesToFile.onWrite(cursor.offset, currentFile))
            numEntries = entriesWritten + 1
            _ <-
              if (numEntries > entriesLimit)
                Pull
                  .eval(swapFile(fileHotswap))
                  .flatMap { case (newCursor, path) => go(tail, fileHotswap, path, newCursor, entriesWritten = 1) }
              else
                go(tail, fileHotswap, currentFile, updatedCursor, numEntries)
          } yield ()

        case None =>
          Pull.done
      }
    }

    in =>
      for {
        (fileHotSwap, fileHandle) <- Stream.resource(Hotswap(Files[F].open(initialFile, Flags.Append)))
        cursor <- Stream.eval(toCursor(fileHandle))
        _ <- go(in, fileHotSwap, initialFile, cursor, initialEntries).stream
      } yield ()
  }

  private[lsht] final case class BytesToFile[F[_]](
      bytebuffer: java.nio.ByteBuffer,
      onWrite: (Offset, Path) => F[Unit]
  )
}
