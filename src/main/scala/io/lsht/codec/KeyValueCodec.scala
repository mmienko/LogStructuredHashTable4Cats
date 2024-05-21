package io.lsht.codec

import cats.effect.Sync
import cats.implicits.*
import io.lsht.{Key, KeyValue}
import fs2.*

import java.nio.ByteBuffer

object KeyValueCodec {

  val ValueSizeSize: Int = 4
  val HeaderSize: Int = CodecUtils.CommonHeaderSize + ValueSizeSize

  def size(kv: KeyValue): Int = HeaderSize + kv.size

  def encode[F[_]: Sync](kv: KeyValue): F[ByteBuffer] = {
    val totalSize = size(kv)

    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .put(0.toByte) // set tombstone to false
          .putInt(kv.key.length)
          .putInt(kv.value.length)
          .put(kv.key.value)
          .put(kv.value)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[KeyValue] = Sync[F].defer {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt
    val _ = bb.get // skip tombstone

    CodecUtils
      .validateCrc(bb, bbSize = bytes.size, checksum)
      .flatMap { _ =>
        Sync[F].delay {
          val keySize = bb.getInt
          val valueSize = bb.getInt
          val key = Array.fill(keySize)(0.toByte)
          bb.get(key)
          val value = Array.fill(valueSize)(0.toByte)
          bb.get(value)

          KeyValue(Key(key), value)
        }
      }
  }

}
