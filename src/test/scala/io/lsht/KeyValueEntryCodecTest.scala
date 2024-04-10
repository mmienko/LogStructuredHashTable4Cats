package io.lsht

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import io.lsht.KeyValueEntryCodec.Offset
import weaver.*

import java.nio.ByteBuffer
import java.util.zip.CRC32C

object KeyValueEntryCodecTest extends SimpleIOSuite {

  private val ChecksumSize = 4
  private val KeyAndValueSizesSize = 8

  test("Encode KeyValueEntry with non-empty key and non-empty value") {
    val entry = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)

    val KeySize = 4
    val ValueSize = 6

    for {
      bb <- KeyValueEntryCodec.encode(entry)

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

  test("Encode KeyValueEntry with non-empty key but empty value") {
    val entry = KeyValueEntry(Key("key1".getBytes), "".getBytes)

    val KeySize = 4
    val ValueSize = 0

    for {
      bb <- KeyValueEntryCodec.encode(entry)

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

  test("Encode KeyValueEntry with empty key but non-empty value") {
    val entry = KeyValueEntry(Key("".getBytes), "value1".getBytes)

    val KeySize = 0
    val ValueSize = 6

    for {
      bb <- KeyValueEntryCodec.encode(entry)

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

  test("Encode KeyValueEntry with empty key and empty value") {
    val entry = KeyValueEntry(Key("".getBytes), "".getBytes)

    val KeySize = 0
    val ValueSize = 0

    for {
      bb <- KeyValueEntryCodec.encode(entry)

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

  test("Decode bytes of non-empty key and non-empty value") {
    val entry = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)

    KeyValueEntryCodec
      .encode(entry)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueEntryCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "key1") and
          expect(new String(resultEntry.value) === "value1")
      }
  }

  test("Decode bytes of non-empty key but empty value") {
    val entry = KeyValueEntry(Key("key1".getBytes), "".getBytes)

    KeyValueEntryCodec
      .encode(entry)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueEntryCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "key1") and
          expect(new String(resultEntry.value) === "")
      }
  }

  test("Decode bytes of empty key but non-empty value") {
    val entry = KeyValueEntry(Key("".getBytes), "value1".getBytes)

    KeyValueEntryCodec
      .encode(entry)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueEntryCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "") and
          expect(new String(resultEntry.value) === "value1")
      }
  }

  test("Decode bytes of empty key and empty value") {
    val entry = KeyValueEntry(Key("".getBytes), "".getBytes)

    KeyValueEntryCodec
      .encode(entry)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueEntryCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "") and
          expect(new String(resultEntry.value) === "")
      }
  }

  test("Decode fails if checksum does not match") {
    val entry = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)

    for {
      bytes <- KeyValueEntryCodec.encode(entry)
      corruptionOffset = ChecksumSize + KeyAndValueSizesSize
      _ <- IO(
        bytes
          .put(corruptionOffset, 1.toByte)
          .put(corruptionOffset + 1, 1.toByte)
      )
      res <- KeyValueEntryCodec.decode(Chunk.byteBuffer(bytes)).attempt
    } yield matches(res) { case Left(error) =>
      expect(error == Errors.Read.BadChecksum)
    }
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
