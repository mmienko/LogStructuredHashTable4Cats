package io.lsht

import cats.effect.*
import cats.syntax.all.*
import weaver.*

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object PutEncoderTest extends SimpleIOSuite {

  private val ChecksumSize = 4
  private val KeyAndValueSizesSize = 8

  test("Encode Put with non-empty key and non-empty value") {
    val put = new PutData {
      override val key: Key = "key1".getBytes
      override val value: Value = "value1".getBytes
    }

    val KeySize = 4
    val ValueSize = 6

    for {
      bb <- PutEncoder.encode(put)

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO(
        bb.slice(ChecksumSize, KeyAndValueSizesSize + KeySize + ValueSize)
      )
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect(valueSize === ValueSize).failFast

      key <- IO(getString(keySize)(bb))
      _ <- expect(key === "key1").failFast

      value <- IO(getString(valueSize)(bb))
    } yield expect(value === "value1")
  }

  test("Encode Put with non-empty key but empty value") {
    val put = new PutData {
      override val key: Key = "key1".getBytes
      override val value: Value = "".getBytes
    }

    val KeySize = 4
    val ValueSize = 0

    for {
      bb <- PutEncoder.encode(put)

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO(
        bb.slice(ChecksumSize, KeyAndValueSizesSize + KeySize + ValueSize)
      )
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect(valueSize === ValueSize).failFast

      key <- IO(getString(keySize)(bb))
      _ <- expect(key === "key1").failFast

      value <- IO(getString(valueSize)(bb))
    } yield expect(value === "")
  }

  test("Encode Put with empty key but non-empty value") {
    val put = new PutData {
      override val key: Key = "".getBytes
      override val value: Value = "value1".getBytes
    }

    val KeySize = 0
    val ValueSize = 6

    for {
      bb <- PutEncoder.encode(put)

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO(
        bb.slice(ChecksumSize, KeyAndValueSizesSize + KeySize + ValueSize)
      )
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect(valueSize === ValueSize).failFast

      key <- IO(getString(keySize)(bb))
      _ <- expect(key === "").failFast

      value <- IO(getString(valueSize)(bb))
    } yield expect(value === "value1")
  }

  test("Encode Put with empty key and empty value") {
    val put = new PutData {
      override val key: Key = "".getBytes
      override val value: Value = "".getBytes
    }

    val KeySize = 0
    val ValueSize = 0

    for {
      bb <- PutEncoder.encode(put)

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO(
        bb.slice(ChecksumSize, KeyAndValueSizesSize + KeySize + ValueSize)
      )
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect(valueSize === ValueSize).failFast

      key <- IO(getString(keySize)(bb))
      _ <- expect(key === "").failFast

      value <- IO(getString(valueSize)(bb))
    } yield expect(value === "")
  }

  private def getString(bytes: Int)(byteBuffer: ByteBuffer): String = {
    val str = Array.fill(bytes)(0.toByte)
    byteBuffer.get(str)
    new String(str)
  }

  private def getChecksum(byteBuffer: ByteBuffer) = IO {
    val bytes = Array.fill(byteBuffer.limit())(0.toByte)
    byteBuffer.get(bytes)
    val crc32c = new CRC32C
    crc32c.update(bytes)
    crc32c.getValue.toInt
  }

}
