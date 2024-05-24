package io.lsht.engine.codec

import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.Path
import io.lsht.engine.codec.KeyValueCodec
import io.lsht.engine.{Key, KeyValue, KeyValueFileReference, Tombstone}
import weaver.*

import java.nio.ByteBuffer

object DataFileDecoderTest extends SimpleIOSuite {

  private val UnusedTestDataFile = Path("/tmp/unused-test-datafile.db")

  test("decode stream of bytes for a single KeyValue") {
    val kv = KeyValue(Key("key1".getBytes), "value1".getBytes)

    fs2.Stream
      .evalUnChunk(KeyValueCodec.encode(kv).map(Chunk.byteBuffer))
      .through(DataFileDecoder.decode)
      .compile
      .lastOrError
      .map { case (result, offset) =>
        matches(result) { case Right(kv: KeyValue) =>
          expect(new String(kv.key.value) === "key1") and
            expect(new String(kv.value) === "value1")
        } and expect(offset === 0)
      }
  }

  test("decode stream of bytes for a single Tombstone") {
    val tombstone = Key("key1")

    fs2.Stream
      .evalUnChunk(TombstoneEncoder.encode(tombstone).map(Chunk.byteBuffer))
      .through(DataFileDecoder.decode)
      .compile
      .lastOrError
      .map { case (result, offset) =>
        matches(result) { case Right(resultTombstone: Tombstone) =>
          expect(new String(resultTombstone.value) === "key1")
        } and expect(offset === 0)
      }
  }

  test("decode stream of bytes for multiple KeyValue's") {
    // TODO: tombstones
    val expectedKv1 = KeyValue(Key("key1".getBytes), "value1".getBytes)
    val expectedKv2 = KeyValue(Key("key2".getBytes), "value2".getBytes)
    val expectedKv3 = KeyValue(Key("key3".getBytes), "value3".getBytes)
    val keyValueEntries = List(
      expectedKv1,
      expectedKv2,
      expectedKv3
    )

    for {
      parsedKeyValueEntries <- fs2.Stream
        .emits(keyValueEntries)
        .evalMap(KeyValueCodec.encode)
        .mapChunks(_.flatMap(Chunk.byteBuffer))
        .through(DataFileDecoder.decode)
        .compile
        .toList
    } yield matches(parsedKeyValueEntries) {
      case (Right(kv1: KeyValue), offset1) ::
          (Right(kv2: KeyValue), offset2) ::
          (Right(kv3: KeyValue), offset3) ::
          Nil =>
        expect.all(
          kv1 === expectedKv1,
          kv2 === expectedKv2,
          kv3 === expectedKv3,
          offset1 === 0L,
          offset2 === KeyValueCodec.HeaderSize + 10,
          offset3 === (KeyValueCodec.HeaderSize + 10) * 2
        )
    }
  }

  test("decode stream of bytes for KeyFileReference") {
    // TODO: tombstones
    val expectedKv1 = KeyValue(Key("key1".getBytes), "value1".getBytes)
    val expectedKv2 = KeyValue(Key("key2".getBytes), "value2".getBytes)
    val expectedKv3 = KeyValue(Key("key3".getBytes), "value3".getBytes)
    val keyValueEntries = List(
      expectedKv1,
      expectedKv2,
      expectedKv3
    )
    val recordSize = KeyValueCodec.HeaderSize + 10

    for {
      parsedFileReferences <- fs2.Stream
        .emits(keyValueEntries)
        .evalMap(KeyValueCodec.encode)
        .mapChunks(_.flatMap(Chunk.byteBuffer))
        .through(DataFileDecoder.decodeAsFileReference(UnusedTestDataFile))
        .compile
        .toList
    } yield matches(parsedFileReferences) {
      case Right((key1, fileRef1)) ::
          Right((key2, fileRef2)) ::
          Right((key3, fileRef3)) ::
          Nil =>
        expect.all(
          key1 === expectedKv1.key,
          key2 === expectedKv2.key,
          key3 === expectedKv3.key,
          fileRef1 === KeyValueFileReference(
            UnusedTestDataFile,
            offset = 0,
            recordSize
          ),
          fileRef2 === KeyValueFileReference(
            UnusedTestDataFile,
            offset = KeyValueCodec.HeaderSize + 10,
            recordSize
          ),
          fileRef3 === KeyValueFileReference(
            UnusedTestDataFile,
            offset = (KeyValueCodec.HeaderSize + 10) * 2,
            recordSize
          )
        )
    }

  }

}
