package io.lsht.codec

import cats.effect.IO

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object CodecCommons {

  val ChecksumSize: Int = 4

  def getString(bytes: Int)(byteBuffer: ByteBuffer): String = {
    val str = Array.fill(bytes)(0.toByte)
    byteBuffer.get(str)
    new String(str)
  }

  def getChecksum(byteBuffer: ByteBuffer): IO[Int] = IO {
    val bytes = Array.fill(byteBuffer.limit())(0.toByte)
    byteBuffer.get(bytes)
    val crc32c = new CRC32C
    crc32c.update(bytes)
    crc32c.getValue.toInt
  }

}
