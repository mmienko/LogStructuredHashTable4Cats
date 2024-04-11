package io.lsht.codec

import cats.effect.IO
import cats.syntax.all.*
import io.lsht.Key
import io.lsht.codec.CodecCommons.*
import io.lsht.codec.TombstoneEncoder
import weaver.*

object TombstoneEncoderTest extends SimpleIOSuite {

  private val NonCrcHeaderSize: Int = 5

  test("Encode non-empty key") {
    val KeySize = 4
    for {
      bb <- TombstoneEncoder.encode(Key("key1"))

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 1.toByte).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect(key === "key1")
  }

  test("Encode empty key") {
    val KeySize = 0
    for {
      bb <- TombstoneEncoder.encode(Key(""))

      crc <- IO(bb.getInt)
      nonChecksumBytes <- IO {
        bb.slice(ChecksumSize, NonCrcHeaderSize + KeySize)
      }
      checkSum <- getChecksum(nonChecksumBytes)
      _ <- expect(crc === checkSum).failFast

      tombstone <- IO(bb.get())
      _ <- expect(tombstone === 1.toByte).failFast

      keySize <- IO(bb.getInt)
      _ <- expect(keySize === KeySize).failFast

      key <- IO(getString(keySize)(bb))
    } yield expect(key === "")
  }
}
