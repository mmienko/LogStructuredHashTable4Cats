package io.lsht.engine.codec

import cats.ApplicativeError
import cats.effect.Sync
import cats.syntax.all.*
import fs2.Chunk

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object CodecUtils {

  val CommonHeaderSize: Int = 4 + // 4-byte CRC
    1 + // 1-byte MetaData, first bit is boolean tombstone
    4 // 4-byte Key Size

  def addCrc[F[_]: Sync](bb: ByteBuffer, totalSize: Int): F[Unit] =
    for {
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
    } yield ()

  def isValidCrc[F[_]: Sync](bytes: Chunk[Byte], checksum: Int): F[Boolean] =
    isValidCrc(bytes.toByteBuffer, bytes.size, checksum)

  def isValidCrc[F[_]: Sync](bb: ByteBuffer, bbSize: Int, checksum: Int): F[Boolean] = Sync[F].delay {
    val crc32c = new CRC32C
    crc32c.update(bb.slice(4, bbSize - 4))
    crc32c.getValue.toInt == checksum
  }

  def validateCrc[F[_]: Sync](bb: ByteBuffer, bbSize: Int, checksum: Int): F[Unit] =
    isValidCrc(bb, bbSize, checksum)
      .flatMap(ApplicativeError[F, Throwable].raiseUnless(_)(BadChecksum))

  def validateCrc[F[_]: Sync](bytes: Chunk[Byte], checksum: Int): F[Unit] =
    validateCrc(bytes.toByteBuffer, bytes.size, checksum)

  object BadChecksum extends Throwable
}
