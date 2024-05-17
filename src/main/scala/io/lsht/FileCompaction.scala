package io.lsht

import cats.data.NonEmptyList
import cats.effect.std.{Console, Hotswap}
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError}
import io.lsht.codec.DataFileDecoder.Tombstone
import io.lsht.codec.{CompactedKeyFileDecoder, DataFileDecoder, KeyValueCodec, ValuesCodec}
import fs2.*
import fs2.io.file.*

import scala.collection.immutable.VectorMap

// TODO: don't expose to public api
object FileCompaction {

  type RecordByFile = (Path, Offset, KeyValue | Tombstone)

  // TODO: immutable.VectorMap vs mutable.LinkedListHashMap
  private val EmptyIndex = VectorMap.empty[Key, CompactedValue | KeyValueFileReference]
  // TODO: there should be a critical section as only 1 compaction process should be running

  def run[F[_]: Async: Console](databaseDirectory: Path): F[Unit] =
    getInactiveDatafiles(databaseDirectory).flatMap { inactiveDataFiles =>
      for {
        now <- Clock[F].realTime

        allCompactedFiles <- getCompactionFiles(databaseDirectory)

        compactedFilesWithIndex <- allCompactedFiles.lastOption.traverse { compactedFiles =>
          Files[F]
            .readAll(compactedFiles.keys)
            .through(CompactedKeyFileDecoder.decode)
            .evalMapFilter {
              case Left(error) =>
                Console[F].println(s"Could not read compacted key. $error").as(none[(Key, CompactedValue)])

              case Right(CompactedKey(key, compactedValue)) =>
                (key, compactedValue).some.pure[F]
            }
            .compile
            .fold(EmptyIndex) { case (index, (key, compactedValue)) => index.updated(key, compactedValue) }
            .tupleLeft(compactedFiles)
        }

        (compactedFiles, compactedIndex) = compactedFilesWithIndex.unzip

        keyValueReferences <- Stream
          .emits(inactiveDataFiles)
          .flatMap { dataFile =>
            Files[F]
              .readAll(dataFile)
              .through(DataFileDecoder.decodeAsFileReference(dataFile))
              .evalMapFilter {
                case Left(err) =>
                  Console[F]
                    .println(s"File ${dataFile.toString} ${err.toString}")
                    .as(none[(Key, KeyValueFileReference) | Tombstone])

                case Right(keyFileRefOrTombstone) =>
                  keyFileRefOrTombstone.some.pure[F]
              }
          }
          .compile
          .fold(compactedIndex.getOrElse(EmptyIndex)) {
            case (index, (key, ref: KeyValueFileReference)) =>
              index.updated(key, ref)

            case (index, key: Tombstone) =>
              index.removed(key)
          }

        _ <- Applicative[F].whenA(keyValueReferences.nonEmpty) {
          Stream
            .fromIterator(keyValueReferences.iterator, chunkSize = 100)
            .through(readKeyValueFromFileReferences(compactedFiles.map(_.values)))
            .through(CompactionFilesUtil.writeKeyValueToCompactionFiles(databaseDirectory, now))
            .compile
            .drain *>
            (inactiveDataFiles ::: asListOfFiles(allCompactedFiles)).traverse_(Files[F].delete)
        }
      } yield ()
    }

  private def getCompactionFiles[F[_]: Async: Console](dir: Path) =
    for {
      filesOrErrors <- CompactionFilesUtil.attemptListCompactionFiles(dir)
      _ <- filesOrErrors
        .collect { case Left(x) => x }
        .traverse_(err => Console[F].println(s"Failed to read a set of compaction files. $err"))
    } yield filesOrErrors.collect { case Right(x) => x }

  private def getInactiveDatafiles[F[_]: Async](dir: Path): F[List[Path]] =
    Files[F].list(dir).compile.toList.map { files =>
      val dataFiles = files.filter(_.fileName.toString.startsWith("data")).toVector
      NonEmptyList
        .fromFoldable(dataFiles.sorted)
        .toList
        .flatMap(_.init)
      // drop the most recent (at end of list due to sort) as it's actively being written to
    }

  private def asListOfFiles(compactionFiles: List[CompactedFiles]): List[Path] = compactionFiles
    .flatMap { case CompactedFiles(key, values, timestamp) => List((key, timestamp), (values, timestamp)) }
    .sortBy(_._2)
    .map(_._1)

  private def readKeyValueFromFileReferences[F[_]: Async](valuesFile: Option[Path]): Pipe[
    F,
    (Key, CompactedValue | KeyValueFileReference),
    KeyValue
  ] = {
    def go(
        s: Stream[F, (Key, CompactedValue | KeyValueFileReference)],
        valuesFileCursor: Option[ReadCursor[F]],
        fileHotSwap: Hotswap[F, ReadCursor[F]],
        location: Option[ReadCursorLocation[F]]
    ): Pull[F, KeyValue, Unit] =
      s.pull.uncons1.flatMap {
        // The Stream of Entries is ordered by File, so once we swap to a new file, we no longer read any of the old files.
        case Some(((key, CompactedValue(offset, length)), tail)) =>
          // TODO: these two Some cases feel the same, but maybe different enough to leave alone
          // TODO: would be nice to have some decode syntax over cursors, instead of having to remember Codec HeaderSize
          for {
            readResult <- valuesFileCursor.get
              .seek(offset)
              .readPull(ValuesCodec.HeaderSize + length)

            bytes <- Pull.eval {
              ApplicativeError[F, Throwable].fromOption(
                readResult.map(_._2), // Ignore updated read cursor, since we seek in loop
                Errors.CompactionException.SeekAndReadFailedOnDataFile(valuesFile.get, offset)
              )
            }

            value <- Pull.eval(ValuesCodec.decode(bytes))

            _ <- Pull.output1(KeyValue(key, value))

            _ <- go(tail, valuesFileCursor, fileHotSwap, location)
          } yield ()

        case Some(((_, KeyValueFileReference(file, offset, length)), tail)) =>
          for {
            location <- location match
              case Some(s @ ReadCursorLocation(currentFile, _)) if file === currentFile =>
                Pull.pure(s)

              // Either we switch to new file or encountered the first Entry in stream
              case _ =>
                Pull.eval {
                  fileHotSwap
                    .swap(Files[F].readCursor(file, Flags.Read))
                    .map(readCursor => ReadCursorLocation(currentFile = file, readCursor))
                }

            readResult <- location.readCursor.seek(offset).readPull(length)

            bytes <- Pull.eval {
              ApplicativeError[F, Throwable].fromOption(
                readResult.map(_._2), // Ignore updated read cursor, since we seek in loop
                Errors.CompactionException.SeekAndReadFailedOnDataFile(file, offset)
              )
            }

            kv <- Pull.eval(KeyValueCodec.decode(bytes))

            _ <- Pull.output1(kv)

            _ <- go(tail, valuesFileCursor, fileHotSwap, location.some)
          } yield ()

        case None =>
          Pull.done
      }

    in =>
      for {
        fileHotSwap <- Stream.resource(Hotswap.create[F, ReadCursor[F]])
        valuesFileCursor <- Stream.resource(valuesFile.traverse(Files[F].readCursor(_, Flags.Read)))
        kv <- go(in, valuesFileCursor, fileHotSwap, none[ReadCursorLocation[F]]).stream
      } yield kv
  }

  private final case class ReadCursorLocation[F[_]](
      currentFile: Path,
      readCursor: ReadCursor[F]
  )

}
