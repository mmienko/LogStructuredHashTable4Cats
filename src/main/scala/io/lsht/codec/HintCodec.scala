package io.lsht.codec

import cats.ApplicativeError
import cats.effect.Sync
import cats.syntax.all.*
import fs2.Chunk
import io.lsht.{EntryHint, Errors, KeyValueEntry, Offset, Key}

import java.nio.ByteBuffer

object HintCodec {

  val HeaderSize: Int = 4 + // 4-byte CRC
    4 + // 4-byte Key Size
    4 + // 4-byte Value Size
    8 // 8-byte Value Offset

  def encode[F[_]: Sync](entry: KeyValueEntry, valuePosition: Offset): F[ByteBuffer] = {
    val totalSize = HeaderSize + entry.key.length
    Sync[F]
      .delay {
        ByteBuffer
          .allocate(totalSize)
          .putInt(0) // zero out CRC
          .putInt(entry.key.length)
          .putInt(entry.value.length)
          .putLong(valuePosition)
          .put(entry.key.value)
          .rewind()
      }
      .flatTap(CodecUtils.addCrc(_, totalSize))
  }

  def decode[F[_]: Sync](bytes: Chunk[Byte]): F[EntryHint] = {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt

    CodecUtils
      .isValidCrc(bb, bbSize = bytes.size, checksum)
      .flatMap(ApplicativeError[F, Throwable].raiseUnless(_)(new HintCodecError(Errors.Read.BadChecksum)))
      .flatMap { _ =>
        Sync[F].delay {
          val keySize = bb.getInt
          val valueSize = bb.getInt
          val valuePosition = bb.getLong
          val key = Array.fill(keySize)(0.toByte)
          bb.get(key)

          EntryHint(Key(key), positionInFile = valuePosition, valueSize)
        }
      }
  }

  class HintCodecError(cause: Throwable) extends Throwable(cause)
}
