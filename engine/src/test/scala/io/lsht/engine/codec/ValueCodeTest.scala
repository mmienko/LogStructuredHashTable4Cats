package io.lsht.engine.codec

import cats.effect.IO
import fs2.Chunk
import CodecCommons.*
import weaver.*

object ValueCodeTest extends SimpleIOSuite {

  test("Encode non-empty Value") {
    for {
      bb <- ValuesCodec.encode[IO]("value".getBytes)
      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
      _ <- expect.eql(crc, checkSum).failFast
      value <- IO(getString(5)(bb))
    } yield expect.eql(value, "value")
  }

  test("Encode empty Value") {
    for {
      bb <- ValuesCodec.encode[IO]("".getBytes)
      crc <- IO(bb.getInt)
      checkSum <- getChecksum(bb.slice())
    } yield expect.eql(crc, checkSum) and expect(!bb.hasRemaining)
  }

  test("Decode bytes of non-empty Value") {
    for {
      bb <- ValuesCodec.encode[IO]("value".getBytes)
      value <- ValuesCodec.decode[IO](Chunk.byteBuffer(bb))
    } yield expect.eql(new String(value), "value")
  }

  test("Decode bytes of empty Value") {
    for {
      bb <- ValuesCodec.encode[IO]("".getBytes)
      value <- ValuesCodec.decode[IO](Chunk.byteBuffer(bb))
    } yield expect.eql(new String(value), "")
  }
}
