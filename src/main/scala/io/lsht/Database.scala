package io.lsht

import cats.effect.*
import cats.effect.std.{Console, Hotswap, Queue, Supervisor}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError, Monad}
import fs2.io.file.*
import fs2.{Chunk, Pipe, Pull, Stream}
import io.lsht.codec.{CompactedKeysFileDecoder, DataFileDecoder, KeyValueCodec, TombstoneEncoder, ValuesCodec}

object Database {

  /*

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

  // TODO: s/Console/Logger/Writer
  def apply[F[_]: Async: Console](directory: Path, entriesLimit: Int = 1000): Resource[F, LogStructuredHashTable[F]] =
    Resource.suspend {
      for {
        _ <- verifyPathIsDirectory[F](directory)

        index <- Ref.ofEffect(loadIndex(directory))

        queue <- Queue.unbounded[F, WriteCommand[F]]

        // TODO: how to handle cancellation? Inside stream or in Supervisor resource?
        //  Terminate queue with None when supervisor closes? Are concurrency bugs avoided?
        seriallyExecuteWrites = Stream
          .fromQueueUnterminated(queue, limit = 1) // TODO: what should limit be? Higher is better for performance
          .evalMap(interpretCommand(index))
          .flattenOption
          .through(executeWriteWithFileRotation(directory, entriesLimit))
          .compile
          .drain

        isClosed <- Ref[F].of(false)

        cancelRemainingCommands = drain(queue).flatMap(_.traverse_(_.complete(DatabaseIsClosed)))
      } yield Supervisor[F](await = false)
        .evalMap(_.supervise(seriallyExecuteWrites))
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
                KeyValueFileReference(
                  dataFile,
                  offset = offset,
                  length = KeyValueCodec.size(kv)
                )
              )

            case (Right(key: Tombstone), _, _) =>
              /*
              This process loads data from oldest to newest, so values are kept until they are deleted. It may
              be more advantageous to instead load from more recent to oldest. Then keys and values are kept
              with certainty and the index updates are purely additive between files. Since data files can
              only be read from beginning of file (the log is like a singly linked list), then within the
              processing of a file, there may be deletes.
               */
              index.removed(key)

            case _ =>
              index
          }
        }
    } yield index

  private def interpretCommand[F[_]: Async: Console: Clock](index: Ref[F, Map[Key, FileReference]])(
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

  // TODO: Rotation threshold could be more sophisticated; instead of a simple count of of entries,
  //  there could be a measure of the number of rewrites to keys. The more rewrites, the more the
  //  file can be compacted, thus saving space during compaction. Database startup times to load
  //  index should also be reduced.
  private[lsht] def executeWriteWithFileRotation[F[_]: Async](
      dbDirectory: Path,
      recordsLimit: Int
  ): Pipe[F, BytesToFile[F], Unit] = {
    // TODO: What happens if the timestamp of new file is smaller than a previous one. Could be due to clock drift.
    //  Enforce that here or document how it could be enforced as it is an unlikely edge case.
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

    // TODO: what happens if file already exists? B/c two programs are running.
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
  // TODO: use in DateFileDecoder?
//  sealed abstract class LoadCommand extends Product with Serializable
//
//  object LoadCommand {
//    final case class Put(kv: KeyValue, offset: Offset) extends LoadCommand
//    final case class Delete(key: Tombstone) extends LoadCommand
//  }
}
