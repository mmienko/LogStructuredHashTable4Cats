package io.lsht

import cats.data.NonEmptyList
import cats.effect.std.{Console, Hotswap}
import cats.effect.{Async, Clock}
import cats.syntax.all.*
import cats.{Applicative, ApplicativeError}
import io.lsht.codec.DataFileDecoder.Tombstone
import io.lsht.codec.{DataFileDecoder, HintFileDecoder, KeyValueEntryCodec, ValuesCodec}
import fs2.*
import fs2.io.file.*

import scala.collection.immutable.VectorMap

// TODO: don't expose to public api
object FileCompaction {

  type EntryByFile = (Path, Offset, KeyValueEntry | Tombstone)

  // TODO: immutable.VectorMap vs mutable.LinkedListHashMap
  private val EmptyIndex = VectorMap.empty[Key, HintFileReference | KeyValueFileReference]
  // TODO: hint vs compacted??
  // TODO: there should be a critical section as only 1 compaction process should be running

  def run[F[_]: Async: Console](databaseDirectory: Path): F[Unit] =
    getImmutableDatafiles(databaseDirectory).flatMap { immutableDataFiles =>
      for {
        now <- Clock[F].realTime

        allCompactedFiles <- getCompactionFiles(databaseDirectory)

        /*
        Read into a index, but value is either HintFileReference | EntryFileReference
        Keys in the immutable files can overwrite keys from compacted files.
        Produce final index by key pointing to compacted files or immutable files.
        Then for each entry, read the values from respective file and write to new compacted file
         */
        compactedFilesWithIndex <- allCompactedFiles.lastOption.traverse { compactedFiles =>
          Files[F]
            .readAll(compactedFiles.hint)
            .through(HintFileDecoder.decode)
            .evalMapFilter {
              case Left(error) =>
                Console[F].println(s"Could not read key-value entry. $error").as(none[(Key, HintFileReference)])

              case Right(EntryHint(key, positionInFile, valueSize)) =>
                (key, HintFileReference(positionInFile, valueSize)).some.pure[F]
            }
            .compile
            .fold(EmptyIndex) { case (index, (key, hintFileReference)) => index.updated(key, hintFileReference) }
            .tupleLeft(compactedFiles)
        }

        (compactedFiles, compactedIndex) = compactedFilesWithIndex.unzip

        entryReferences <- Stream
          .emits(immutableDataFiles)
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
            case (index, (key, entry: KeyValueFileReference)) =>
              index.updated(key, entry)

            case (index, key: Tombstone) =>
              index.removed(key)
          }

        _ <- Applicative[F].whenA(entryReferences.nonEmpty) {
          Stream
            .fromIterator(entryReferences.iterator, chunkSize = 100)
            .through(readKeyValueEntryFromDataFile(compactedFiles.map(_.values)))
            .through(CompactionFilesUtil.writeKeyValueEntries(databaseDirectory, now))
            .compile
            .drain *>
            (immutableDataFiles ::: asListOfFiles(allCompactedFiles)).traverse_(Files[F].delete)
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

  private def getImmutableDatafiles[F[_]: Async](dir: Path): F[List[Path]] =
    Files[F].list(dir).compile.toList.map { files =>
      val dataFiles = files.filter(_.fileName.toString.startsWith("data")).toVector
      NonEmptyList
        .fromFoldable(dataFiles.sorted)
        .toList
        .flatMap(_.init)
      // drop the most recent (at end of list due to sort) as it's actively being written to
    }

  private def asListOfFiles(compactionFiles: List[CompactedFiles]): List[Path] = compactionFiles
    .flatMap { case CompactedFiles(hint, values, timestamp) => List((hint, timestamp), (values, timestamp)) }
    .sortBy(_._2)
    .map(_._1)

  private def readKeyValueEntryFromDataFile[F[_]: Async](valuesFile: Option[Path]): Pipe[
    F,
    (Key, HintFileReference | KeyValueFileReference),
    KeyValueEntry
  ] = {
    def go(
        s: Stream[F, (Key, HintFileReference | KeyValueFileReference)],
        valuesFileCursor: Option[ReadCursor[F]],
        fileHotSwap: Hotswap[F, ReadCursor[F]],
        location: Option[ReadCursorLocation[F]]
    ): Pull[F, KeyValueEntry, Unit] =
      s.pull.uncons1.flatMap {
        // The Stream of Entries is ordered by File, so once we swap to a new file, we no longer read any of the old files.
        case Some(((key, HintFileReference(positionInFile, valueSize)), tail)) =>
          // TODO: these two Some cases feel the same, but maybe different enough to leave alone
          // TODO: would be nice to have some decode syntax over cursors, instead of having to remember Codec HeaderSize
          for {
            readResult <- valuesFileCursor.get
              .seek(positionInFile)
              .readPull(ValuesCodec.HeaderSize + valueSize)

            bytes <- Pull.eval {
              ApplicativeError[F, Throwable].fromOption(
                readResult.map(_._2), // Ignore updated read cursor, since we seek in loop
                Errors.CompactionException.SeekAndReadFailedOnDataFile(valuesFile.get, positionInFile)
              )
            }

            value <- Pull.eval(ValuesCodec.decode(bytes))

            _ <- Pull.output1(KeyValueEntry(key, value))

            _ <- go(tail, valuesFileCursor, fileHotSwap, location)
          } yield ()

        case Some(((_, KeyValueFileReference(filePath, positionInFile, entrySize)), tail)) =>
          for {
            location <- location match
              case Some(s @ ReadCursorLocation(currentFile, _)) if filePath === currentFile =>
                Pull.pure(s)

              // Either we switch to new file or encountered the first Entry in stream
              case _ =>
                Pull.eval {
                  fileHotSwap
                    .swap(Files[F].readCursor(filePath, Flags.Read))
                    .map(readCursor => ReadCursorLocation(currentFile = filePath, readCursor))
                }

            readResult <- location.readCursor.seek(positionInFile).readPull(entrySize)

            bytes <- Pull.eval {
              ApplicativeError[F, Throwable].fromOption(
                readResult.map(_._2), // Ignore updated read cursor, since we seek in loop
                Errors.CompactionException.SeekAndReadFailedOnDataFile(filePath, positionInFile)
              )
            }

            entry <- Pull.eval(KeyValueEntryCodec.decode(bytes))

            _ <- Pull.output1(entry)

            _ <- go(tail, valuesFileCursor, fileHotSwap, location.some)
          } yield ()

        case None =>
          Pull.done
      }

    in =>
      for {
        fileHotSwap <- Stream.resource(Hotswap.create[F, ReadCursor[F]])
        valuesFileCursor <- Stream.resource(valuesFile.traverse(Files[F].readCursor(_, Flags.Read)))
        keyValueEntry <- go(in, valuesFileCursor, fileHotSwap, none[ReadCursorLocation[F]]).stream
      } yield keyValueEntry
  }

  private final case class ReadCursorLocation[F[_]](
      currentFile: Path,
      readCursor: ReadCursor[F]
  )

  private case class HintFileReference(positionInFile: Offset, valueSize: Int)

}
