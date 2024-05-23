package io.lsht

import cats.data.Validated
import cats.effect.Async
import cats.effect.std.Console
import cats.syntax.all.*
import fs2.io.file.*
import fs2.{Chunk, Pipe, Pull, Stream}
import io.lsht.codec.{CompactedKeyCodec, CompactedKeysFileDecoder, ValuesCodec}
import io.lsht.{KeyValue, given_Ordering_Path}

import scala.concurrent.duration.FiniteDuration

object CompactionFilesUtil {

  def writeKeyValueToCompactionFiles[F[_]: Async](
      databaseDirectory: Path,
      timestamp: FiniteDuration
  ): Pipe[F, KeyValue, Unit] = {
    val fileTimestamp = timestamp.toMillis.toString
    in =>
      in.through(writeCompactedValues(databaseDirectory / s"values.$fileTimestamp.db"))
        .evalMap(CompactedKeyCodec.encode)
        .mapChunks(_.flatMap(Chunk.byteBuffer))
        .through(Files[F].writeAll(databaseDirectory / s"keys.$fileTimestamp.db", Flags.Append))
  }

  def readKeyValueEntries[F[_]: Async](compactedFiles: CompactedFiles): Stream[F, Either[Throwable, KeyValue]] =
    Files[F]
      .readAll(compactedFiles.keys)
      .through(CompactedKeysFileDecoder.decode)
      .through(readValuesAndCombine(compactedFiles.values))

  def getValidCompactionFiles[F[_]: Async: Console](dir: Path): F[List[CompactedFiles]] =
    for {
      filesOrErrors <- CompactionFilesUtil.attemptListCompactionFiles(dir)
      _ <- filesOrErrors
        .collect { case Left(x) => x }
        .traverse_(err => Console[F].println(s"Failed to read a set of compaction files. $err"))
    } yield filesOrErrors.collect { case Right(x) => x }

  def attemptListCompactionFiles[F[_]: Async](dir: Path): F[List[Either[String, CompactedFiles]]] =
    Files[F]
      .list(dir)
      .compile
      .toVector
      .map { files =>
        files
          .filter { path =>
            val fn = path.fileName.toString
            fn.startsWith("keys") || fn.startsWith("values")
          }
          .sorted
          .toList
          .partition(_.fileName.toString.startsWith("keys"))
          .mapN { case (keysFile, valuesFile) =>
            val keysFileName = keysFile.fileName.toString
            val keysTs = keysFileName.split("\\.").toList match
              case "keys" :: timestamp :: "db" :: Nil =>
                timestamp.toLongOption
              case _ =>
                none[Long]
            val valuesFileName = valuesFile.fileName.toString
            val valuesTs = valuesFileName.split("\\.").toList match
              case "values" :: timestamp :: "db" :: Nil =>
                timestamp.toLongOption
              case _ =>
                none[Long]

            (
              Validated.fromOption(keysTs, s"Key File name, $keysFileName, could not be parsed").toValidatedNec,
              Validated.fromOption(valuesTs, s"Value File name, $valuesFileName, could not be parsed").toValidatedNec,
              Validated
                .cond(
                  keysTs == valuesTs,
                  (),
                  s"Keys, $keysFileName, and Values, $valuesFileName, timestamps are not equal"
                )
                .toValidatedNec
            ).mapN { case (ts, _, _) =>
              CompactedFiles(keysFile, valuesFile, ts)
            }.leftMap(_.mkString_("\n"))
              .toEither
          }
      }

  private def writeCompactedValues[F[_]: Async](path: Path): Pipe[F, KeyValue, (KeyValue, Offset)] = {
    def go(s: Stream[F, KeyValue], cursor: WriteCursor[F]): Pull[F, (KeyValue, Offset), Unit] =
      s.pull.uncons1.flatMap {
        case Some((entry, tail)) =>
          Pull
            .eval(ValuesCodec.encode(entry.value))
            .map(Chunk.byteBuffer)
            .flatMap(cursor.writePull)
            .flatMap(updatedCursor => Pull.output1((entry, cursor.offset)) >> go(tail, updatedCursor))

        case None =>
          Pull.done
      }

    in =>
      Stream
        .resource(Files[F].writeCursor(path, Flags.Append))
        .flatMap(go(in, _).stream)
  }

  private def readValuesAndCombine[F[_]: Async](
      compactedKeysFile: Path
  ): Pipe[F, Either[Throwable, CompactedKey], Either[Throwable, KeyValue]] = {
    def go(
        s: Stream[F, Either[Throwable, CompactedKey]],
        cursor: ReadCursor[F]
    ): Pull[F, Either[Throwable, KeyValue], Unit] =
      s.pull.uncons1.flatMap {
        case Some((Left(error), tail)) =>
          Pull.output1(error.asLeft[KeyValue]) >> go(tail, cursor)

        case Some((Right(CompactedKey(key, CompactedValue(offset, length))), tail)) =>
          cursor
            .seek(offset)
            .readPull(ValuesCodec.HeaderSize + length)
            .map(_.map(_._2))
            .evalMap {
              case Some(bytes) =>
                ValuesCodec
                  .decode(bytes)
                  .attempt
                  .map(_.map(value => KeyValue(key, value)))

              case None =>
                new Throwable("Value is missing").asLeft[KeyValue].pure[F]
            }
            .flatMap(Pull.output1) >> go(tail, cursor)

        case None =>
          Pull.done
      }

    in =>
      Stream
        .resource(Files[F].readCursor(compactedKeysFile, Flags.Read))
        .flatMap(go(in, _).stream)
  }

}
