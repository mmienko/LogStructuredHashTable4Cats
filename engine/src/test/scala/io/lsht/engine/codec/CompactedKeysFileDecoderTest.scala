package io.lsht.engine.codec

import cats.effect.IO
import fs2.Chunk
import io.lsht
import io.lsht.engine.{CompactedKey, CompactedValue, Key, KeyValue}
import weaver.*

object CompactedKeysFileDecoderTest extends SimpleIOSuite {

  test("decode stream of bytes for a single KeyValue") {
    val key = Key("key")
    fs2.Stream
      .eval(CompactedKeyCodec.encode[IO](KeyValue(key, "value".getBytes), valuePosition = 0))
      .mapChunks(_.flatMap(Chunk.byteBuffer))
      .through(CompactedKeysFileDecoder.decode)
      .compile
      .lastOrError
      .map(whenSuccess(_) { compactedKey =>
        expect.eql(compactedKey, CompactedKey(key, CompactedValue(offset = 0, length = 5)))
      })
  }

  test("decode stream of bytes for multiple KeyValue's") {
    fs2.Stream
      .emits((0 until 5).map(i => KeyValue(Key(s"key$i"), "value".getBytes)).toList)
      .zipWithIndex
      .covary[IO]
      .evalMap { case (kv, i) => CompactedKeyCodec.encode[IO](kv, valuePosition = i * 10) }
      .mapChunks(_.flatMap(Chunk.byteBuffer))
      .through(CompactedKeysFileDecoder.decode)
      .compile
      .toList
      .map(_.zipWithIndex)
      .flatTap(list => expect.eql(list.length, 5).failFast)
      .map(forEach(_) { case (res, i) =>
        whenSuccess(res) { compactedKey =>
          expect.eql(compactedKey, CompactedKey(Key(s"key$i"), CompactedValue(offset = i * 10, length = 5)))
        }
      })
  }
}
