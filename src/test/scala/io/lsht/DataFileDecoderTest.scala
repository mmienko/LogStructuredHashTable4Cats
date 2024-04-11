package io.lsht

import cats.syntax.all.*
import fs2.Chunk
import weaver.*

import java.nio.ByteBuffer

object DataFileDecoderTest extends SimpleIOSuite {

  private val ChecksumSize = 4
  private val KeyAndValueSizesSize = 8

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

//  test("decode stream of bytes for multiple KeyValueEntry's") {
//    val entry1 = KeyValueEntry(Key("key1".getBytes), "value1".getBytes)
//    val entry2 = KeyValueEntry(Key("key2".getBytes), "value2".getBytes)
//    val entry3 = KeyValueEntry(Key("key3".getBytes), "value3".getBytes)
//
//    def stream(entry: KeyValueEntry) = fs2.Stream.evalUnChunk(KeyValueEntryCodec.encode(entry).map(Chunk.byteBuffer))
//
//    (stream(entry1) ++ stream(entry2) ++ stream(entry3))
//      .through(DataFileDecoder.decode)
//      .compile
//      .toList
//      .map {
//        case (res1, o1) :: (res2, o2) :: (res3, o3) :: Nil =>
//          matches(res1) { case Right(resultEntry) =>
//            expect(new String(resultEntry.key.value) === "key1") and
//              expect(new String(resultEntry.value) === "value1")
//          } and expect(o1 === 0) and
//            matches(res2) { case Right(resultEntry) =>
//              expect(new String(resultEntry.key.value) === "key2") and
//                expect(new String(resultEntry.value) === "value2")
//            } and expect(o2 === (ChecksumSize + KeyAndValueSizesSize + 10)) and
//            matches(res3) { case Right(resultEntry) =>
//              expect(new String(resultEntry.key.value) === "key3") and
//                expect(new String(resultEntry.value) === "value3")
//            } and expect(o3 === ((ChecksumSize + KeyAndValueSizesSize + 10) * 2))
//
//        case x =>
//          failure(s"Expecting 3 results, but instead got ${x.size}")
//      }
//  }

}
