package io.lsht.codec

import cats.effect.Sync
import cats.syntax.all.*
import fs2.{Pipe, Pull}
import io.lsht.{CompactedKey, CompactedValue, Key}

object CompactedKeysFileDecoder {

  type ParsedCompactedKey = Either[Throwable, CompactedKey]

  private final case class HeaderState(crc: Int, keySize: Int, valueSize: Int, valueOffset: Long)

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedCompactedKey] = {
    def go(s: fs2.Stream[F, Byte], headerState: Option[HeaderState]): Pull[F, ParsedCompactedKey, Unit] = {
      headerState match
        case None =>
          s.pull.unconsN(CompactedKeyCodec.HeaderSize).flatMap {
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
                CompactedKey(Key(bytes.toArray), CompactedValue(offset = header.valueOffset, length = header.valueSize))
                  .asRight[Throwable]
              ) >> go(tail, headerState = None)

            case None =>
              Pull.raiseError(new Throwable("Missing Key bytes, file corrupted"))
          }
    }

    in => go(in, headerState = None).stream
  }
}
