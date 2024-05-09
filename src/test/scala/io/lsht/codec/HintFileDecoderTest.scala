package io.lsht.codec

import cats.effect.IO
import fs2.Chunk
import io.lsht.{EntryHint, Key, KeyValueEntry}
import weaver.*

object HintFileDecoderTest extends SimpleIOSuite {

  test("decode stream of bytes for a single KeyValueEntry") {
    val key = Key("key")
    fs2.Stream
      .eval(HintCodec.encode[IO](KeyValueEntry(key, "value".getBytes), valuePosition = 0))
      .mapChunks(_.flatMap(Chunk.byteBuffer))
      .through(HintFileDecoder.decode)
      .compile
      .lastOrError
      .map(whenSuccess(_) { hint =>
        expect.eql(hint, EntryHint(key, positionInFile = 0, valueSize = 5))
      })
  }

  test("decode stream of bytes for multiple KeyValueEntry's") {
    fs2.Stream
      .emits((0 until 5).map(i => KeyValueEntry(Key(s"key$i"), "value".getBytes)).toList)
      .zipWithIndex
      .covary[IO]
      .evalMap { case (entry, i) => HintCodec.encode[IO](entry, valuePosition = i * 10) }
      .mapChunks(_.flatMap(Chunk.byteBuffer))
      .through(HintFileDecoder.decode)
      .compile
      .toList
      .map(_.zipWithIndex)
      .flatTap(list => expect.eql(list.length, 5).failFast)
      .map(forEach(_) { case (res, i) =>
        whenSuccess(res) { hint =>
          expect.eql(hint, EntryHint(Key(s"key$i"), positionInFile = i * 10, valueSize = 5))
        }
      })
  }
}
