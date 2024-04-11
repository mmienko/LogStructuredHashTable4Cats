package io.lsht

import cats.effect.Sync
import fs2.{Chunk, Pipe, Pull}
import cats.syntax.all.*
import io.lsht.KeyValueEntryCodec.ValueSizeSize

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
    def go(
        s: fs2.Stream[F, Byte],
        currentOffset: Offset,
        headerState: Option[HeaderState]
    ): Pull[F, ParsedKeyValueEntry, Unit] = {
      headerState match
        /*
        Read the Common Header and determine if we should decode a Tombstone or Key-Value entry. Otherwise we are at
        the end of the stream.
         */
        case None =>
          s.pull.unconsN(CodecUtils.CommonHeaderSize).flatMap {
            case Some((headerBytes, tail)) =>
              val bb = headerBytes.toByteBuffer
              val checksum = bb.getInt
              val isTombstone = bb.get() == 1.toByte
              val keySize: KeySize = bb.getInt

              val nextState =
                if isTombstone then HeaderState.Tombstone(currentOffset, checksum, keySize, headerBytes)
                else HeaderState.KeyOnly(currentOffset, keySize, headerBytes)
              go(tail, currentOffset = currentOffset + headerBytes.size, nextState.some)

            case None =>
              Pull.done
          }

        // Read the rest of Tombstone and decode it, or error
        case Some(HeaderState.Tombstone(offset, checksum, keySize, headerBytes)) =>
          s.pull.unconsN(keySize).flatMap {
            case Some((keyBytes, tail)) =>
              Pull
                .eval(CodecUtils.isValidCrc[F](bytes = headerBytes ++ keyBytes, checksum))
                .flatMap(isValid =>
                  if isValid then Pull.pure(Key(keyBytes.toArray)) else Pull.raiseError(Errors.Startup.BadChecksum)
                )
                .attempt
                .flatMap(key => Pull.output1((key, offset)))
                >> go(tail, currentOffset = currentOffset + keySize, headerState = None)

            case None =>
              Pull.raiseError(Errors.Startup.MissingTombstoneKey)
          }

        // Read the value size and go to read the rest of Key-Value entry, or error
        case Some(ks @ HeaderState.KeyOnly(_, _, _)) =>
          s.pull.unconsN(ValueSizeSize).flatMap {
            case Some((valueSizeBytes, tail)) =>
              go(
                tail,
                currentOffset = currentOffset + ValueSizeSize,
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
          val dataSize = keyHeader.keySize + valueSize
          s.pull.unconsN(dataSize).flatMap {
            case Some((entryBytes, tail)) =>
              val allBytes = keyHeader.bytes ++ valueSizeBytes ++ entryBytes
              Pull
                .eval(KeyValueEntryCodec.decode(allBytes))
                .adaptErr { case Errors.Read.BadChecksum => Errors.Startup.BadChecksum }
                .attempt
                .flatMap(kv => Pull.output1((kv, keyHeader.offset)))
                >> go(tail, currentOffset = currentOffset + dataSize, headerState = None)

            case None =>
              Pull.raiseError(Errors.Startup.MissingKeyValueEntry)
          }
    }

    in => go(in, currentOffset = 0, headerState = None).stream
  }

}
