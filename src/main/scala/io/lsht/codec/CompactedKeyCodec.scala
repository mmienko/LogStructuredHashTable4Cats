package io.lsht.codec

import cats.effect.Sync
import cats.syntax.all.*
import fs2.Chunk
import io.lsht.*

import java.nio.ByteBuffer

object CompactedKeyCodec {

  val HeaderSize: Int = 4 + // 4-byte CRC
    4 + // 4-byte Key Size
    4 + // 4-byte Value Size
    8 // 8-byte Value Offset

  def encode[F[_]: Sync](kv: KeyValue, valuePosition: Offset): F[ByteBuffer] = {
    val totalSize = HeaderSize + kv.key.length
    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .putInt(kv.key.length)
          .putInt(kv.value.length)
          .putLong(valuePosition)
          .put(kv.key.value)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[CompactedKey] = {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt

    CodecUtils
      .validateCrc(bb, bbSize = bytes.size, checksum)
      .flatMap { _ =>
        Sync[F].delay {
          val keySize = bb.getInt
          val valueSize = bb.getInt
          val valuePosition = bb.getLong
          val key = Array.fill(keySize)(0.toByte)
          bb.get(key)

          CompactedKey(Key(key), CompactedValue(valuePosition, valueSize))
        }
      }
  }

}
