package io.lsht

import cats.data.Validated
import cats.effect.Async
import cats.syntax.all.*
import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.io.file.{Files, Flags, Path, ReadCursor, WriteCursor}
import io.lsht.codec.{HintCodec, HintFileDecoder, ValuesCodec}
import io.lsht.{KeyValue, given_Ordering_Path}

import scala.concurrent.duration.FiniteDuration

object CompactionFilesUtil {

  def writeKeyValueEntries[F[_]: Async](
      databaseDirectory: Path,
      timestamp: FiniteDuration
  ): Pipe[F, KeyValue, Unit] = {
    val fileTimestamp = timestamp.toMillis.toString
    in =>
      in.through(writeValuesAndGiveHints(databaseDirectory / s"values.$fileTimestamp.db"))
        .evalMap(HintCodec.encode)
        .mapChunks(_.flatMap(Chunk.byteBuffer))
        .through(Files[F].writeAll(databaseDirectory / s"hint.$fileTimestamp.db", Flags.Append))
  }

  def readKeyValueEntries[F[_]: Async](compactedFiles: CompactedFiles): Stream[F, Either[Throwable, KeyValue]] =
    Files[F]
      .readAll(compactedFiles.hint)
      .through(HintFileDecoder.decode)
      .through(readValuesAndCombine(compactedFiles.values))

  def attemptListCompactionFiles[F[_]: Async](dir: Path): F[List[Either[String, CompactedFiles]]] =
    Files[F]
      .list(dir)
      .compile
      .toVector
      .map { files =>
        files
          .filter { path =>
            val fn = path.fileName.toString
            fn.startsWith("hint") || fn.startsWith("values")
          }
          .sorted
          .toList
          .partition(_.fileName.toString.startsWith("hint"))
          .mapN { case (hintFile, valuesFile) =>
            val hintFileName = hintFile.fileName.toString
            val hintTs = hintFileName.split("\\.").toList match
              case "hint" :: timestamp :: "db" :: Nil =>
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
              Validated.fromOption(hintTs, s"Hint File name, $hintFileName, could not be parsed").toValidatedNec,
              Validated.fromOption(valuesTs, s"Value File name, $valuesFileName, could not be parsed").toValidatedNec,
              Validated
                .cond(
                  hintTs == valuesTs,
                  (),
                  s"Hint, $hintFileName, and Values, $valuesFileName, timestamps are not equal"
                )
                .toValidatedNec
            ).mapN { case (ts, _, _) =>
              CompactedFiles(hintFile, valuesFile, ts)
            }.leftMap(_.mkString_("\n"))
              .toEither
          }
      }

  private def writeValuesAndGiveHints[F[_]: Async](path: Path): Pipe[F, KeyValue, (KeyValue, Offset)] = {
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
      hintFile: Path
  ): Pipe[F, Either[Throwable, EntryHint], Either[Throwable, KeyValue]] = {
    def go(
        s: Stream[F, Either[Throwable, EntryHint]],
        cursor: ReadCursor[F]
    ): Pull[F, Either[Throwable, KeyValue], Unit] =
      s.pull.uncons1.flatMap {
        case Some((Left(error), tail)) =>
          Pull.output1(error.asLeft[KeyValue]) >> go(tail, cursor)

        case Some((Right(hint), tail)) =>
          cursor
            .seek(hint.positionInFile)
            .readPull(ValuesCodec.HeaderSize + hint.valueSize)
            .map(_.map(_._2))
            // TODO: better errors
            .evalMap {
              case Some(bytes) =>
                ValuesCodec
                  .decode(bytes)
                  .attempt
                  .map(_.map(value => KeyValue(hint.key, value)))

              case None =>
                new Throwable("Value is missing").asLeft[KeyValue].pure[F]
            }
            .flatMap(Pull.output1) >> go(tail, cursor)

        case None =>
          Pull.done
      }

    in =>
      Stream
        .resource(Files[F].readCursor(hintFile, Flags.Read))
        .flatMap(go(in, _).stream)
  }

}
