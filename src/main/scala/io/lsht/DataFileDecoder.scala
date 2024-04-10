package io.lsht

import cats.effect.Sync
import fs2.{Chunk, Pipe, Pull}
import cats.syntax.all._

object DataFileDecoder {

  type Offset = Long
  type ParsedKeyValueEntry = (Either[Throwable, KeyValueEntry], Offset)
  private type KeySize = Int
  private type ValueSize = Int
  private type Header = (Offset, KeySize, ValueSize, Chunk[Byte])

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedKeyValueEntry] = {
    def go(s: fs2.Stream[F, (Byte, Long)], header: Option[Header]): Pull[F, ParsedKeyValueEntry, Unit] = {
      header match
        case None =>
          s.pull.unconsN(KeyValueEntryCodec.HeaderSize).flatMap {
            // Start of the stream || start of new key-value entry?
            case Some((headerBytesWithOffset, tail)) =>
              val offset: Offset = headerBytesWithOffset.head.get._2
              val headerBytes = headerBytesWithOffset.map(_._1)
              val bb = headerBytes.toByteBuffer
              val _ = bb.getInt // skip crc until full key-value entry is extracted
              val keySize: KeySize = bb.getInt
              val valueSize: ValueSize = bb.getInt
              go(tail, header = (offset, keySize, valueSize, headerBytes).some)

            // Cleanly finished the stream
            case None =>
              Pull.done
          }

        case Some((offset, keySize, valueSize, headerBytes)) =>
          s.pull.unconsN(keySize + valueSize).flatMap {
            // Given Header, read the actual key-value data
            case Some((entryBytesWithOffset, tail)) =>
              Pull
                .eval(KeyValueEntryCodec.decode(headerBytes ++ entryBytesWithOffset.map(_._1)))
                .adaptErr { case Errors.Read.BadChecksum => Errors.Startup.BadChecksum }
                .attempt
                .flatMap(put => Pull.output1((put, offset))) >> go(tail, header = None)

            // Header is present, but NOT the key-value data, which means stream was corrupted.
            case None =>
              Pull.raiseError(Errors.Startup.MissingKeyValueEntry)
          }
    }

    in => go(in.zipWithIndex, header = None).stream
  }

}
