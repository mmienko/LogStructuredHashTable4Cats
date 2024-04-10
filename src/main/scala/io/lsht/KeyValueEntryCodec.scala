package io.lsht

import cats.effect.Sync
import cats.implicits.*
import fs2.*

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object KeyValueEntryCodec {

  val HeaderSize: Int = CommonHeaderSize + 4 // 4-byte Value Size

  def size(entry: KeyValueEntry): Int = HeaderSize + entry.size
  
  def encode[F[_]: Sync](entry: KeyValueEntry): F[ByteBuffer] = {
    val totalSize = size(entry)

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

}
