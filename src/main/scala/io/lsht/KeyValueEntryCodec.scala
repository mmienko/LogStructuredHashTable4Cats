package io.lsht

import cats.ApplicativeError
import cats.effect.Sync
import cats.implicits.*
import fs2.*

import java.nio.ByteBuffer

object KeyValueEntryCodec {

  val ValueSizeSize: Int = 4
  val HeaderSize: Int = CodecUtils.CommonHeaderSize + ValueSizeSize

  def size(entry: KeyValueEntry): Int = HeaderSize + entry.size

  def encode[F[_]: Sync](entry: KeyValueEntry): F[ByteBuffer] = {
    val totalSize = size(entry)

    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .put(0.toByte) // set tombstone to false
          .putInt(entry.key.length)
          .putInt(entry.value.length)
          .put(entry.key.value)
          .put(entry.value)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[KeyValueEntry] = Sync[F].defer {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt
    val _ = bb.get // skip tombstone

    CodecUtils
      .isValidCrc(bb, bbSize = bytes.size, checksum)
      .flatMap(ApplicativeError[F, Throwable].raiseUnless(_)(Errors.Read.BadChecksum))
      .flatMap { _ =>
        Sync[F].delay {
          val keySize = bb.getInt
          val valueSize = bb.getInt
          val key = Array.fill(keySize)(0.toByte)
          bb.get(key)
          val value = Array.fill(valueSize)(0.toByte)
          bb.get(value)

          KeyValueEntry(Key(key), value)
        }
      }
  }

}
