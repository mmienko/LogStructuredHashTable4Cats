package io.lsht.codec

import cats.effect.Sync
import cats.syntax.all.*
import fs2.io.file.Path
import fs2.{Chunk, Pipe, Pull}
import io.lsht.*
import io.lsht.codec.KeyValueCodec.ValueSizeSize

object DataFileDecoder {

  type Tombstone = Key
  // TODO: Adt's instead of |
  type ParsedKeyValue = ParsedHeaderState[KeyValue]
  type ParsedKeyValueFileReference = Either[Throwable, (Key, KeyValueFileReference) | Tombstone]
  private type ParsedHeaderState[A] = (Either[Throwable, A | Tombstone], Offset)
  private type KeySize = Int
  private type ValueSize = Int

  private sealed trait HeaderState

  private object HeaderState {
    final case class KeyOnly(offset: Offset, keySize: KeySize, bytes: Chunk[Byte]) extends HeaderState
    final case class KeyValue(keyHeader: KeyOnly, valueSize: ValueSize, bytes: Chunk[Byte]) extends HeaderState {
      def dataSize: Int = keyHeader.keySize + valueSize
    }
    final case class Tombstone(offset: Offset, checksum: Int, keySize: KeySize, bytes: Chunk[Byte]) extends HeaderState
  }

  def decodeAsFileReference[F[_]: Sync](dataFile: Path): Pipe[F, Byte, ParsedKeyValueFileReference] = {
    val p = decodeKeyValueState { kvHeader =>
      val keyHeader = kvHeader.keyHeader
      KeyValuePull[F, (Key, KeyValueFileReference)](
        keyHeader.keySize,
        keyBytes =>
          Pull
            .eval(
              decodeKeyValueFileReference(
                dataFile,
                keyHeader.offset,
                entrySize = kvHeader.dataSize,
                bytes = keyHeader.bytes ++ kvHeader.bytes ++ keyBytes
              )
            )
            .adaptErr { case Errors.Read.BadChecksum => Errors.Startup.BadChecksum }
      )
    }

    in => in.through(p).map(_._1)
  }

  def decode[F[_]: Sync]: Pipe[F, Byte, ParsedKeyValue] = {
    decodeKeyValueState { kvHeader =>
      val keyHeader = kvHeader.keyHeader
      val dataSize = keyHeader.keySize + kvHeader.valueSize
      KeyValuePull(
        numberOfBytesFromEntryToPull = dataSize,
        entryBytes =>
          Pull
            .eval(KeyValueCodec.decode(keyHeader.bytes ++ kvHeader.bytes ++ entryBytes))
            .adaptErr { case Errors.Read.BadChecksum => Errors.Startup.BadChecksum }
      )
    }
  }

  private def decodeKeyValueState[F[_]: Sync, A](
      pullKeyValue: HeaderState.KeyValue => KeyValuePull[F, A]
  ): Pipe[F, Byte, ParsedHeaderState[A]] = {
    def go(
        s: fs2.Stream[F, Byte],
        currentOffset: Offset,
        headerState: Option[HeaderState]
    ): Pull[F, ParsedHeaderState[A], Unit] = {
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
              Pull.output1((Errors.Startup.MissingValueSize.asLeft[A | Tombstone], ks.offset))
          }

        // Read the full Key-Value entry and decode it, or error
        case Some(kv @ HeaderState.KeyValue(keyHeader, _, _)) =>
          val KeyValuePull(numberOfBytesFromEntryToPull, handleBytes) = pullKeyValue(kv)
          s.pull.unconsN(numberOfBytesFromEntryToPull).flatMap {
            case Some((entryBytes, tail)) =>
              val unconsumed = kv.dataSize - numberOfBytesFromEntryToPull
              for {
                res <- handleBytes(entryBytes).attempt
                _ <- Pull.output1((res, keyHeader.offset))
                _ <- go(
                  s = tail.drop(unconsumed),
                  currentOffset = currentOffset + kv.dataSize,
                  headerState = None
                )
              } yield ()

            case None =>
              Pull.output1((Errors.Startup.MissingKeyValueEntry.asLeft[A | Tombstone], keyHeader.offset))
          }
    }

    in => go(in, currentOffset = 0, headerState = None).stream
  }

  private def decodeKeyValueFileReference[F[_]: Sync](
      filePath: Path,
      offset: Offset,
      entrySize: Int,
      bytes: Chunk[Byte]
  ): F[(Key, KeyValueFileReference)] = Sync[F].defer {
    val bb = bytes.toByteBuffer
    val checksum = bb.getInt
    val _ = bb.get // skip tombstone

    // TODO: how to enforce CRC? Probably need to read whole value
    Sync[F].delay {
      val keySize = bb.getInt
      val valueSize = bb.getInt
      val key = Array.fill(keySize)(0.toByte)
      bb.get(key)

      (Key(key), KeyValueFileReference(filePath, offset, KeyValueCodec.HeaderSize + entrySize))
    }
  }

  private case class KeyValuePull[F[_], A](
      numberOfBytesFromEntryToPull: Int,
      handleBytes: Chunk[Byte] => Pull[F, Nothing, A]
  )

}
