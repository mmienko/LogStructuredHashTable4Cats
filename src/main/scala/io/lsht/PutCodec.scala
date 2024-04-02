package io.lsht

import cats.effect.Sync
import cats.implicits.*
import fs2.Chunk

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object PutCodec {
  val MetaDataByteSize: Int = 4 + // 4-byte CRC
    4 + // 4-byte Key Size
    4 // 4-byte Value Size
  // TODO?:    4 + // 4-byte Timestamp

  def encode[F[_]: Sync](put: PutData): F[ByteBuffer] = {
    val totalSize = MetaDataByteSize + put.dataSize

    for {
      bb <- Sync[F].delay(
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .putInt(put.key.length)
          .putInt(put.value.length)
          .put(put.key)
          .put(put.value)
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

  def decode[F[_] : Sync](bytes: Chunk[Byte]): F[PutValue] = Sync[F].delay {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt

    val crc32c = new CRC32C
    crc32c.update(bb.slice(4, bytes.size - 4))
    if crc32c.getValue.toInt != checksum then
      throw Errors.Read.BadChecksum

    val keySize = bb.getInt
    val valueSize = bb.getInt
    val key = Array.fill(keySize)(0.toByte)
    bb.get(key)
    val value = Array.fill(valueSize)(0.toByte)
    bb.get(value)

    PutValue(key, value)
  }

}
