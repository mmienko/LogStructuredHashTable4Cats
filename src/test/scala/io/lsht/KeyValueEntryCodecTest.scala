package io.lsht

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import weaver.*

import java.nio.ByteBuffer
import java.util.zip.CRC32C
import io.lsht.CodecCommons._

object KeyValueEntryCodecTest extends SimpleIOSuite {

  private val NonCrcHeaderSize: Int = 9

  test("Encode KeyValueEntry with non-empty key and non-empty value") {
    val entry = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)

    val KeySize = 4
    val ValueSize = 6

    for {
      bb <- KeyValueEntryCodec.encode(entry)

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize + ValueSize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 0.toByte).failFast

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
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize + ValueSize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 0.toByte).failFast

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
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize + ValueSize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 0.toByte).failFast

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
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize + ValueSize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 0.toByte).failFast

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
      corruptionOffset = ChecksumSize + NonCrcHeaderSize
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

}
