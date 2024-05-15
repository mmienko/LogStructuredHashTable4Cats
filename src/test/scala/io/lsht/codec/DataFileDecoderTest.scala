package io.lsht.codec

import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.Path
import io.lsht.codec.CodecCommons.ChecksumSize
import io.lsht.codec.{DataFileDecoder, KeyValueEntryCodec, TombstoneEncoder}
import io.lsht.{EntryFileReference, Key, KeyValueEntry}
import weaver.*

import java.nio.ByteBuffer

object DataFileDecoderTest extends SimpleIOSuite {

  private val UnusedTestDataFile = Path("/tmp/unused-test-datafile.db")

  test("decode stream of bytes for a single KeyValueEntry") {
    val entry = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)

    fs2.Stream
      .evalUnChunk(KeyValueEntryCodec.encode(entry).map(Chunk.byteBuffer))
      .through(DataFileDecoder.decode)
      .compile
      .lastOrError
      .map { case (result, offset) =>
        matches(result) { case Right(resultEntry: KeyValueEntry) =>
          expect(new String(resultEntry.key.value) === "key1") and
            expect(new String(resultEntry.value) === "value1")
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
        matches(result) { case Right(resultTombstone: DataFileDecoder.Tombstone) =>
          expect(new String(resultTombstone.value) === "key1")
        } and expect(offset === 0)
      }
  }

  test("decode stream of bytes for multiple KeyValueEntry's") {
    // TODO: tombstones
    val expectedKv1 = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)
    val expectedKv2 = KeyValueEntry(Key("key2".getBytes), "value2".getBytes)
    val expectedKv3 = KeyValueEntry(Key("key3".getBytes), "value3".getBytes)
    val keyValueEntries = List(
      expectedKv1,
      expectedKv2,
      expectedKv3
    )

    for {
      parsedKeyValueEntries <- fs2.Stream
        .emits(keyValueEntries)
        .evalMap(KeyValueEntryCodec.encode)
        .mapChunks(_.flatMap(Chunk.byteBuffer))
        .through(DataFileDecoder.decode)
        .compile
        .toList
    } yield matches(parsedKeyValueEntries) {
      case (Right(kv1: KeyValueEntry), offset1) ::
          (Right(kv2: KeyValueEntry), offset2) ::
          (Right(kv3: KeyValueEntry), offset3) ::
          Nil =>
        expect.all(
          kv1 === expectedKv1,
          kv2 === expectedKv2,
          kv3 === expectedKv3,
          offset1 === 0L,
          offset2 === KeyValueEntryCodec.HeaderSize + 10,
          offset3 === (KeyValueEntryCodec.HeaderSize + 10) * 2
        )
    }
  }

  test("decode stream of bytes for KeyFileReference") {
    // TODO: tombstones
    val expectedKv1 = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)
    val expectedKv2 = KeyValueEntry(Key("key2".getBytes), "value2".getBytes)
    val expectedKv3 = KeyValueEntry(Key("key3".getBytes), "value3".getBytes)
    val keyValueEntries = List(
      expectedKv1,
      expectedKv2,
      expectedKv3
    )
    val entrySize = KeyValueEntryCodec.HeaderSize + 10

    for {
      parsedFileReferences <- fs2.Stream
        .emits(keyValueEntries)
        .evalMap(KeyValueEntryCodec.encode)
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
          fileRef1 === EntryFileReference(
            UnusedTestDataFile,
            positionInFile = 0,
            entrySize
          ),
          fileRef2 === EntryFileReference(
            UnusedTestDataFile,
            positionInFile = KeyValueEntryCodec.HeaderSize + 10,
            entrySize
          ),
          fileRef3 === EntryFileReference(
            UnusedTestDataFile,
            positionInFile = (KeyValueEntryCodec.HeaderSize + 10) * 2,
            entrySize
          )
        )
    }

  }

}
