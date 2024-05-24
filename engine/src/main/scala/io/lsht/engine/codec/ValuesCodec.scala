package io.lsht.engine.codec

import cats.effect.Sync
import cats.syntax.all.*
import fs2.Chunk
import io.lsht.engine.Value

import java.nio.ByteBuffer

object ValuesCodec {

  val HeaderSize: Int = 4 // 4-byte CRC

  def encode[F[_]: Sync](values: Value): F[ByteBuffer] = {
    val totalSize = HeaderSize + values.length
    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .put(values)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[Value] = {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt

    CodecUtils
      .validateCrc(bb, bbSize = bytes.size, checksum)
      .map { _ =>
        val value = Array.fill(bytes.size - HeaderSize)(0.toByte)
        bb.get(value)
        value
      }
  }

}
