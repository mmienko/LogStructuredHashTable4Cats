package io.lsht

import cats.effect.Sync
import fs2.{Chunk, Pipe, Pull}
import cats.syntax.all._

object DataFileDecoder {

  type Offset = Long
  type Tombstone = Key
  type ParsedKeyValueEntry = (Either[Throwable, KeyValueEntry | Tombstone], Offset)
  private type KeySize = Int
  private type ValueSize = Int

  private sealed trait HeaderState

  private object HeaderState {
    final case class KeyOnly(offset: Offset, keySize: KeySize, bytes: Chunk[Byte]) extends HeaderState
    final case class KeyValue(keyHeader: KeyOnly, valueSize: ValueSize, bytes: Chunk[Byte]) extends HeaderState
    final case class Tombstone(offset: Offset, checksum: Int, keySize: KeySize, bytes: Chunk[Byte]) extends HeaderState
  }

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedKeyValueEntry] = {
    def go(s: fs2.Stream[F, (Byte, Long)], headerState: Option[HeaderState]): Pull[F, ParsedKeyValueEntry, Unit] = {
      headerState match
        /*
        Read the Common Header and determine if we should decode a Tombstone or Key-Value entry. Otherwise we are at
        the end of the stream.
         */
        case None =>
          s.pull.unconsN(CodecUtils.CommonHeaderSize).flatMap {
            case Some((headerBytesWithOffset, tail)) =>
              val offset: Offset = headerBytesWithOffset.head.get._2
              val headerBytes = headerBytesWithOffset.map(_._1)
              val bb = headerBytes.toByteBuffer
              val checksum = bb.getInt
              val isTombstone = bb.get() == 1.toByte
              val keySize: KeySize = bb.getInt

              val nextState =
                if isTombstone then HeaderState.Tombstone(offset, checksum, keySize, headerBytes)
                else HeaderState.KeyOnly(offset, keySize, headerBytes)
              go(tail, nextState.some)

            case None =>
              Pull.done
          }

        // Read the rest of Tombstone and decode it, or error
        case Some(HeaderState.Tombstone(offset, checksum, keySize, headerBytes)) =>
          s.pull.unconsN(keySize).flatMap {
            case Some((keyBytesWithOffset, tail)) =>
              val keyBytes = keyBytesWithOffset.map(_._1)
              Pull
                .eval(CodecUtils.isValidCrc[F](bytes = headerBytes ++ keyBytes, checksum))
                .flatMap(isValid =>
                  if isValid then Pull.pure(Key(keyBytes.toArray)) else Pull.raiseError(Errors.Startup.BadChecksum)
                )
                .attempt
                .flatMap(key => Pull.output1((key, offset)) >> go(tail, headerState = None))

            case None =>
              Pull.raiseError(Errors.Startup.MissingTombstoneKey)
          }

        // Read the value size and go to read the rest of Key-Value entry, or error
        case Some(ks @ HeaderState.KeyOnly(_, _, _)) =>
          s.pull.unconsN(4).flatMap {
            case Some((valueSizeBytesWithOffset, tail)) =>
              val valueSizeBytes = valueSizeBytesWithOffset.map(_._1)
              go(
                tail,
                headerState = HeaderState
                  .KeyValue(
                    ks,
                    valueSize = valueSizeBytes.toByteBuffer.getInt,
                    bytes = valueSizeBytes
                  )
                  .some
              )

            case None =>
              Pull.raiseError(Errors.Startup.MissingValueSize)
          }

        // Read the full Key-Value entry and decode it, or error
        case Some(HeaderState.KeyValue(keyHeader, valueSize, valueSizeBytes)) =>
          s.pull.unconsN(keyHeader.keySize + valueSize).flatMap {
            case Some((entryBytesWithOffset, tail)) =>
              val allBytes = keyHeader.bytes ++ valueSizeBytes ++ entryBytesWithOffset.map(_._1)
              Pull
                .eval(KeyValueEntryCodec.decode(allBytes))
                .adaptErr { case Errors.Read.BadChecksum => Errors.Startup.BadChecksum }
                .attempt
                .flatMap(kv => Pull.output1((kv, keyHeader.offset))) >> go(tail, headerState = None)

            case None =>
              Pull.raiseError(Errors.Startup.MissingKeyValueEntry)
          }
    }

    // TODO: might be cleaner to pass offset rather than zipWithIndex
    in => go(in.zipWithIndex, headerState = None).stream
  }

}
