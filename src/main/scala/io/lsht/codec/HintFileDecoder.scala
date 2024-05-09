package io.lsht.codec

import cats.effect.Sync
import fs2.{Pipe, Pull}
import io.lsht.{EntryHint, Key}
import cats.syntax.all._

object HintFileDecoder {

  type ParsedEntryHint = Either[Throwable, EntryHint]

  private final case class HeaderState(crc: Int, keySize: Int, valueSize: Int, valueOffset: Long)

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedEntryHint] = {
    def go(s: fs2.Stream[F, Byte], headerState: Option[HeaderState]): Pull[F, ParsedEntryHint, Unit] = {
      headerState match
        case None =>
          s.pull.unconsN(HintCodec.HeaderSize).flatMap {
            case Some((bytes, tail)) =>
              val bb = bytes.toByteBuffer
              go(
                tail,
                HeaderState(
                  crc = bb.getInt,
                  keySize = bb.getInt,
                  valueSize = bb.getInt,
                  valueOffset = bb.getLong
                ).some
              )

            case None =>
              Pull.done
          }

        case Some(header) =>
          s.pull.unconsN(header.keySize).flatMap {
            case Some((bytes, tail)) =>
              Pull.output1(
                EntryHint(Key(bytes.toArray), positionInFile = header.valueOffset, header.valueSize).asRight[Throwable]
              ) >> go(tail, headerState = None)

            case None =>
              Pull.raiseError(new Throwable("Missing Key bytes, file corrupted"))
          }
    }

    in => go(in, headerState = None).stream
  }
}
