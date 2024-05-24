package io.lsht

import cats.data.NonEmptyList
import cats.effect.std.{Console, Hotswap}
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError}
import io.lsht.codec.{CompactedKeysFileDecoder, DataFileDecoder, KeyValueCodec, ValuesCodec}
import fs2.*
import fs2.io.file.*

import scala.collection.immutable.VectorMap

object FileCompaction {

  type RecordByFile = (Path, Offset, KeyValue | Tombstone)

  private val EmptyIndex = VectorMap.empty[Key, CompactedValue | KeyValueFileReference]

  /*
  Compaction is broken up into 2 phases, load meta data, then ETL records in streaming fashion
  into compacted files.
  Load all Compacted Key File References (metadata) for the compacted data files. Then,
  load all KeyValue File References (metadata) for all inactive data files. Finally, in a
  streaming fashion, ETL each individual record to the compacted files. This "phasing" saves space
  as not all values need to be kept in memory at the same time.
   */
  def run[F[_]: Async: Console](databaseDirectory: Path): F[Unit] =
    getInactiveDatafiles(databaseDirectory).flatMap { inactiveDataFiles =>
      for {
        now <- Clock[F].realTime

        allCompactedFiles <- CompactionFilesUtil.getValidCompactionFiles(databaseDirectory)

        compactedFilesWithIndex <- allCompactedFiles.lastOption.traverse { compactedFiles =>
          Files[F]
            .readAll(compactedFiles.keys)
            .through(CompactedKeysFileDecoder.decode)
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
          for {
            readResult <- valuesFileCursor.get
              .seek(offset)
              .readPull(ValuesCodec.HeaderSize + length)

            bytes <- Pull.eval {
              ApplicativeError[F, Throwable].fromOption(
                readResult.map(_._2), // Ignore updated read cursor, since we seek in loop
                CompactionException.SeekAndReadFailedOnDataFile(valuesFile.get, offset)
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
                CompactionException.SeekAndReadFailedOnDataFile(file, offset)
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

  class CompactionException(message: String) extends Throwable(message)

  object CompactionException {
    class SeekAndReadFailedOnDataFile(file: Path, offset: Offset)
        extends CompactionException(s"Could not read Corrupted DataFile ${file.toString} at offset ${offset.toString}")
  }

}
