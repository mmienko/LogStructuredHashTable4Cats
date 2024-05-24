package io.lsht.engine.codec

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import CodecCommons.*
import io.lsht.engine.{Key, KeyValue}
import weaver.*

import java.nio.ByteBuffer

object KeyValueCodecTest extends SimpleIOSuite {

  private val NonCrcHeaderSize: Int = 9

  test("Encode KeyValue with non-empty key and non-empty value") {
    val kv = KeyValue(Key("key1".getBytes), "value1".getBytes)

    val KeySize = 4
    val ValueSize = 6

    for {
      bb <- KeyValueCodec.encode(kv)

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

  test("Encode KeyValue with non-empty key but empty value") {
    val kv = KeyValue(Key("key1".getBytes), "".getBytes)

    val KeySize = 4
    val ValueSize = 0

    for {
      bb <- KeyValueCodec.encode(kv)

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

  test("Encode KeyValue with empty key but non-empty value") {
    val kv = KeyValue(Key("".getBytes), "value1".getBytes)

    val KeySize = 0
    val ValueSize = 6

    for {
      bb <- KeyValueCodec.encode(kv)

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

  test("Encode KeyValue with empty key and empty value") {
    val kv = KeyValue(Key("".getBytes), "".getBytes)

    val KeySize = 0
    val ValueSize = 0

    for {
      bb <- KeyValueCodec.encode(kv)

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
    val kv = KeyValue(Key("key1".getBytes), "value1".getBytes)

    KeyValueCodec
      .encode(kv)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "key1") and
          expect(new String(resultEntry.value) === "value1")
      }
  }

  test("Decode bytes of non-empty key but empty value") {
    val kv = KeyValue(Key("key1".getBytes), "".getBytes)

    KeyValueCodec
      .encode(kv)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "key1") and
          expect(new String(resultEntry.value) === "")
      }
  }

  test("Decode bytes of empty key but non-empty value") {
    val kv = KeyValue(Key("".getBytes), "value1".getBytes)

    KeyValueCodec
      .encode(kv)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "") and
          expect(new String(resultEntry.value) === "value1")
      }
  }

  test("Decode bytes of empty key and empty value") {
    val kv = KeyValue(Key("".getBytes), "".getBytes)

    KeyValueCodec
      .encode(kv)
      .map(Chunk.byteBuffer)
      .flatMap(KeyValueCodec.decode)
      .map { resultEntry =>
        expect(new String(resultEntry.key.value) === "") and
          expect(new String(resultEntry.value) === "")
      }
  }

  test("Decode fails if checksum does not match") {
    val kv = KeyValue(Key("key1".getBytes), "value1".getBytes)

    for {
      bytes <- KeyValueCodec.encode(kv)
      corruptionOffset = ChecksumSize + NonCrcHeaderSize
      _ <- IO(
        bytes
          .put(corruptionOffset, 1.toByte)
          .put(corruptionOffset + 1, 1.toByte)
      )
      res <- KeyValueCodec.decode(Chunk.byteBuffer(bytes)).attempt
    } yield matches(res) { case Left(error) =>
      expect(error == CodecUtils.BadChecksum)
    }
  }

}
