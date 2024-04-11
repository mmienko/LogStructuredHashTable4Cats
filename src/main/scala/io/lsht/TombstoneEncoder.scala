package io.lsht

import cats.effect.Sync
import cats.syntax.all.*
import io.lsht.CodecUtils.CommonHeaderSize

import java.nio.ByteBuffer

object TombstoneEncoder {

  def encode[F[_]: Sync](key: Key): F[ByteBuffer] = {
    val totalSize = CommonHeaderSize + key.length

    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .put(1.toByte) // set tombstone to true
          .putInt(key.length)
          .put(key.value)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

}
