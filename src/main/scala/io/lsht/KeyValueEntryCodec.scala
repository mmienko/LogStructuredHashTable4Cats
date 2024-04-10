package io.lsht

import cats.effect.Sync
import cats.implicits.*
import fs2._

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object KeyValueEntryCodec {

  type Offset = Long
  type ParsedKeyValueEntry = (Either[Throwable, KeyValueEntry], Offset)
  private type KeySize = Int
  private type ValueSize = Int
  private type Header = (Offset, KeySize, ValueSize, Chunk[Byte])

  val HeaderSize: Int = 4 + // 4-byte CRC
    4 + // 4-byte Key Size
    4 // 4-byte Value Size
  // TODO?:    4 + // 4-byte Timestamp

  def encode[F[_]: Sync](entry: KeyValueEntry): F[ByteBuffer] = {
    val totalSize = HeaderSize + entry.size

    for {
      bb <- Sync[F].delay(
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .putInt(entry.key.length)
          .putInt(entry.value.length)
          .put(entry.key.value)
          .put(entry.value)
          .rewind()
      )

      checksum <- Sync[F].delay {
        val crc32c = new CRC32C
        crc32c.update(bb.slice(4, totalSize - 4))
        crc32c.getValue
      }

      _ <- Sync[F].delay {
        /*
        Java's CheckSum returns a long but CRC32C actually returns an integer.
        Copy the bottom 4 bytes of long (the int part) to the ByteBuffer
         */
        var cs: Long = checksum
        for i <- 0 until 4 do {
          bb.put(3 - i, (cs & 0xff).toByte)
          cs >>= 8
        }
      }
    } yield bb
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[KeyValueEntry] = Sync[F].delay {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt

    val crc32c = new CRC32C
    crc32c.update(bb.slice(4, bytes.size - 4))
    if crc32c.getValue.toInt != checksum then throw Errors.Read.BadChecksum

    val keySize = bb.getInt
    val valueSize = bb.getInt
    val key = Array.fill(keySize)(0.toByte)
    bb.get(key)
    val value = Array.fill(valueSize)(0.toByte)
    bb.get(value)

    KeyValueEntry(Key(key), value)
  }

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedKeyValueEntry] = {
    def go(s: fs2.Stream[F, (Byte, Long)], header: Option[Header]): Pull[F, ParsedKeyValueEntry, Unit] = {
      header match
        case None =>
          s.pull.unconsN(HeaderSize).flatMap {
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
                .eval(decode(headerBytes ++ entryBytesWithOffset.map(_._1)))
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
