package io.lsht.codec

import cats.effect.IO
import fs2.Chunk
import io.lsht.codec.CodecCommons.*
import io.lsht.codec.KeyValueEntryCodecTest.expect
import io.lsht.{EntryHint, Key, KeyValueEntry}
import weaver.*

object HintCodecTest extends SimpleIOSuite {

  test("Encodes Entry with offset and non-empty key and non-empty value") {
    for {
      bb <- HintCodec.encode(KeyValueEntry(Key("key"), "value".getBytes), valuePosition = 1971L)

      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
      _ <- expect.eql(crc, checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect.eql(keySize, 3).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect.eql(valueSize, 5).failFast

      valueOffset <- IO(bb.getLong)
      _ <- expect.eql(valueOffset, 1971L).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect.eql(key, "key")
  }

  test("Encodes Entry with offset and non-empty key and empty value") {
    for {
      bb <- HintCodec.encode(KeyValueEntry(Key("key"), "".getBytes), valuePosition = 1971L)

      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
      _ <- expect.eql(crc, checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect.eql(keySize, 3).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect.eql(valueSize, 0).failFast

      valueOffset <- IO(bb.getLong)
      _ <- expect.eql(valueOffset, 1971L).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect.eql(key, "key")
  }

  test("Encodes Entry with offset and empty key and non-empty value") {
    for {
      bb <- HintCodec.encode(KeyValueEntry(Key(""), "value".getBytes), valuePosition = 1971L)

      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
      _ <- expect.eql(crc, checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect.eql(keySize, 0).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect.eql(valueSize, 5).failFast

      valueOffset <- IO(bb.getLong)
      _ <- expect.eql(valueOffset, 1971L).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect.eql(key, "")
  }

  test("Encodes Entry with offset and empty key and empty value") {
    for {
      bb <- HintCodec.encode(KeyValueEntry(Key(""), "".getBytes), valuePosition = 1971L)

      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
      _ <- expect.eql(crc, checkSum).failFast

      keySize <- IO(bb.getInt)
      _ <- expect.eql(keySize, 0).failFast

      valueSize <- IO(bb.getInt)
      _ <- expect.eql(valueSize, 0).failFast

      valueOffset <- IO(bb.getLong)
      _ <- expect.eql(valueOffset, 1971L).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect.eql(key, "")
  }

  // TODO: lots of common patterns, unify. Also test crc detection

  test("decode bytes to EntryHint") {
    HintCodec
      .encode(KeyValueEntry(Key("key"), "value".getBytes), valuePosition = 1971L)
      .map(Chunk.byteBuffer)
      .flatMap(HintCodec.decode[IO])
      .map { case EntryHint(key, positionInFile, valueSize) =>
        expect.eql(new String(key.value), "key") and
          expect.eql(positionInFile, 1971L) and
          expect.eql(valueSize, 5)
      }
  }
}
