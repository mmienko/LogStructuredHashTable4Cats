package io.lsht.engine

import cats.effect.*
import cats.effect.std.{Console, Hotswap, Queue, Supervisor}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monad}
import fs2.io.file.*
import fs2.io.file.Watcher.Event
import fs2.{Chunk, Pipe, Pull, Stream}
import io.lsht.engine
import io.lsht.engine.codec.{CompactedKeysFileDecoder, DataFileDecoder, KeyValueCodec, TombstoneEncoder, ValuesCodec}

import scala.concurrent.duration.*

object Database {

  def apply[F[_]: Async: Console](
      directory: Path,
      entriesLimit: Int,
      compactionWatchPollTimeout: FiniteDuration
  ): Resource[F, LogStructuredHashTable[F]] = apply(directory, entriesLimit, compactionWatchPollTimeout.some)

  /**
    * Simple Database with optional compaction. Good method for testing
    * @param directory
    *   main DB directory
    * @param entriesLimit
    *   number of entries in the active data file before it rotates
    * @param compactionWatchPollTimeout
    *   time for poll for new data file must wait before retrying. If set, then compaction is started in background.
    * @tparam F
    *   IO
    * @return
    *   LogStructuredHashTable, a HashTable backed by log
    */
  def apply[F[_]: Async: Console](
      directory: Path,
      entriesLimit: Int = 1000,
      compactionWatchPollTimeout: Option[FiniteDuration] = None
  ): Resource[F, LogStructuredHashTable[F]] =
    Resource.suspend {
      for {
        _ <- verifyPathIsDirectory[F](directory)

        index <- Ref.ofEffect(loadIndex(directory))

        queue <- Queue.unbounded[F, WriteCommand[F]]

        seriallyExecuteWrites = Stream
          .fromQueueUnterminated(queue, limit = 1)
          .evalMapFilter(interpretCommand(index))
          .through(executeWriteWithFileRotation(directory, entriesLimit))
          .compile
          .drain

        isClosed <- Ref[F].of(false)

        cancelRemainingCommands = drain(queue).flatMap(_.traverse_(_.complete(DatabaseIsClosed)))

      } yield Supervisor[F](await = false)
        .evalTap { sup =>
          compactionWatchPollTimeout.traverse_(to => sup.supervise(startCompactionInBackground(directory, to)))
        }
        .evalTap(_.supervise(seriallyExecuteWrites))
        .onFinalize(isClosed.set(true) >> cancelRemainingCommands)
        .as(new LogStructuredHashTable(queue, index, isClosed))
    }

  private def verifyPathIsDirectory[F[_]: Async](directory: Path) =
    Files[F]
      .isDirectory(directory, followLinks = false)
      .flatMap {
        ApplicativeError[F, Throwable]
          .raiseUnless(_)(PathNotADirectory)
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

  private def loadIndex[F[_]: Async: Console](directory: Path): F[Map[Key, FileReference]] =
    for {
      files <- getFiles(directory)

      compactedFiles <- CompactionFilesUtil.getValidCompactionFiles(directory).map(_.lastOption)

      initialIndex <- (compactedFiles match
        case Some(CompactedFiles(keysFile, valuesFile, _)) =>
          Files[F]
            .readAll(keysFile)
            .through(CompactedKeysFileDecoder.decode)
            .evalMapFilter {
              case Left(error) =>
                Console[F]
                  .println(s"Startup Error during loading index: ${error.toString} in file $keysFile")
                  .as(none[(Key, CompactedValueReference)])

              case Right(CompactedKey(key, CompactedValue(offset, length))) =>
                (key, CompactedValueReference(file = valuesFile, offset, length + ValuesCodec.HeaderSize)).some.pure[F]
            }

        case None =>
          Stream.empty
      ).compile
        .fold(Map.empty[Key, FileReference]) { case (index, (key, fr)) => index.updated(key, fr) }

      dataFiles = files.filter(_.fileName.toString.startsWith("data.")).sorted

      index <- Stream
        .emits(dataFiles)
        .flatMap { dataFile =>
          Files[F]
            .readAll(dataFile)
            .through(DataFileDecoder.decode[F])
            .map(x => (x._1, x._2, dataFile))
        }
        .evalTap {
          case (Left(err), offset, file) =>
            Console[F]
              .println(s"Startup Error during loading index: ${err.toString} at offset $offset in file $file")
          case _ =>
            Applicative[F].unit
        }
        .compile
        .fold(initialIndex) { case (index, res) =>
          res match {
            case (Right(kv: KeyValue), offset, dataFile) =>
              index.updated(
                kv.key,
                engine.KeyValueFileReference(
                  dataFile,
                  offset = offset,
                  length = KeyValueCodec.size(kv)
                )
              )

            case (Right(key: Tombstone), _, _) =>
              index.removed(key)

            case _ =>
              index
          }
        }
    } yield index

  private def startCompactionInBackground[F[_]: Async: Console](
      directory: Path,
      compactionWatchPollTimeout: FiniteDuration
  ) =
    Files[F]
      .watch(
        directory,
        types = List[Watcher.EventType](Watcher.EventType.Created),
        modifiers = Nil,
        pollTimeout = compactionWatchPollTimeout
      )
      .filter(_.isInstanceOf[Event.Created])
      .drop(1) // active data file is always created so skip it
      .sliding(size = 2, step = 2) // every 2 files, run compaction
      .map(_.size)
      .filter(_ === 2) // Ignore last file
      .evalMap(_ => FileCompaction.run(directory))
      .compile
      .drain

  private def interpretCommand[F[_]: Async: Console: Clock](index: Ref[F, Map[Key, FileReference]])(
      cmd: WriteCommand[F]
  ): F[Option[BytesToFile[F]]] =
    cmd match
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

  private[lsht] def executeWriteWithFileRotation[F[_]: Async](
      dbDirectory: Path,
      recordsLimit: Int
  ): Pipe[F, BytesToFile[F], Unit] = {
    val nextDataFileName =
      Clock[F].realTime.map(now => dbDirectory / s"data.${now.toMillis.toString}.db")

    def nextDataFile(name: Path) = Files[F]
      .open(name, Flags.Append)
      .evalMap(Files[F].writeCursorFromFileHandle(_, append = true))

    def rotateFile(fileHotswap: Hotswap[F, WriteCursor[F]]) =
      nextDataFileName.flatMap { fn =>
        fileHotswap
          .swap(nextDataFile(fn))
          .tupleRight(fn)
      }

    def go(
        s: Stream[F, BytesToFile[F]],
        cursorHotswap: Hotswap[F, WriteCursor[F]],
        currentFile: Path,
        cursor: WriteCursor[F],
        recordsWritten: Int
    ): Pull[F, Unit, Unit] = {
      s.pull.uncons1.flatMap {
        case Some((bytesToFile, tail)) =>
          for {
            nextCursor <- cursor.writePull(Chunk.byteBuffer(bytesToFile.bytebuffer))
            _ <- Pull.eval(bytesToFile.onWrite(cursor.offset, currentFile))
            totalRecords = recordsWritten + 1
            _ <-
              if (totalRecords > recordsLimit)
                Pull
                  .eval(rotateFile(cursorHotswap))
                  .flatMap { case (newCursor, path) => go(tail, cursorHotswap, path, newCursor, recordsWritten = 1) }
              else
                go(tail, cursorHotswap, currentFile, nextCursor, totalRecords)
          } yield ()

        case None =>
          Pull.done
      }
    }

    in =>
      for {
        activeDataFile <- Stream.eval(nextDataFileName)
        (cursorHotswap, cursor) <- Stream.resource(Hotswap(nextDataFile(activeDataFile)))
        _ <- go(in, cursorHotswap, currentFile = activeDataFile, cursor, recordsWritten = 0).stream
      } yield ()
  }

  private[lsht] final case class BytesToFile[F[_]](
      bytebuffer: java.nio.ByteBuffer,
      onWrite: (Offset, Path) => F[Unit]
  )

  object PathNotADirectory extends Throwable
  object DatabaseIsClosed extends Throwable
}
