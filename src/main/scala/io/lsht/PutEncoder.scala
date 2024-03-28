package io.lsht

import cats.effect.Sync
import cats.implicits._

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object PutEncoder {
  val MetaDataByteSize: Int = 4 + // 4-byte CRC
    4 + // 4-byte Key Size
    4 // 4-byte Value Size
  // TODO?:    4 + // 4-byte Timestamp

  def encode[F[_]: Sync](put: Put[F]): F[ByteBuffer] = {
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
        var cs = checksum
        for (i <- 0 until 4) {
          bb.put(4 - i, (cs & 0xff).toByte)
          cs >>= 8
        }
      }
    } yield bb
  }
}
