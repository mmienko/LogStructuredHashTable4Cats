package io.lsht.engine

import cats.Show
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.{Files, Flags, Path}
import CompactionFilesUtil.attemptListCompactionFiles
import FileCompactionTest.failure
import io.lsht.engine.codec.{KeyValueCodec, TombstoneEncoder}
import weaver.{Expect, Expectations}

import scala.concurrent.duration.FiniteDuration

object TestUtils {

  given Show[Array[Byte]] = Show.show(new String(_))
  given Show[Key] = Show.show(k => s"Key(${k.value.show})")
//  given Show[Value] = Show.show(v => s"Value(${new String(v)})")
  given Show[KeyValue] = Show.show(e => s"KV(${e.key.show}, ${e.value.show})")

  val DataFileNamePattern: String = "data\\.\\d*\\.db"

  given StringKeyOrdering: Ordering[Key] = Ordering.String.contramap[Key](k => new String(k.value))

  def expectSomeString(expected: String)(actual: Option[Array[Byte]]): Expectations =
    (new Expect).eql(expected.some, actual.map(new String(_)))

  def expectString(expected: String)(actual: Array[Byte]): Expectations =
    (new Expect).eql(expected, new String(actual))

  def tempDatabase: Resource[IO, LogStructuredHashTable[IO]] =
    tempDirectory.flatMap(Database[IO](_))

  def tempDirectory: Resource[IO, Path] =
    Files[IO].tempDirectory

  def tempDatabaseWithDir: Resource[IO, (LogStructuredHashTable[IO], Path)] =
    tempDirectory.flatMap(dir => Database[IO](dir).tupleRight(dir))

  def putAll(db: LogStructuredHashTable[IO])(keyValuePairs: KeyValue*): IO[Unit] =
    keyValuePairs.traverse_(kv => db.put(kv.key, kv.value))

  def writeToCompactionFile(dir: Path, timestamp: FiniteDuration, keyValuePairs: KeyValue*): IO[Unit] =
    fs2.Stream
      .emits(keyValuePairs)
      .through(CompactionFilesUtil.writeKeyValueToCompactionFiles[IO](dir, timestamp))
      .compile
      .drain

  def appendEntriesToDataFile(file: Path, keyValueEntries: KeyValue*): IO[Unit] =
    appendToDataFile(file, keyValueEntries.map(_.asRight[Tombstone])*)

  def appendTombstonesToDataFile(file: Path, tombstones: Tombstone*): IO[Unit] =
    appendToDataFile(file, tombstones.map(_.asLeft[KeyValue])*)

  def appendToDataFile(file: Path, keyValueEntries: Either[Tombstone, KeyValue]*): IO[Unit] =
    fs2.Stream
      .evals(keyValueEntries.pure[IO])
      .evalMap(_.fold(TombstoneEncoder.encode[IO], KeyValueCodec.encode[IO]))
      .mapChunks(_.flatMap(Chunk.byteBuffer))
      .through(Files[IO].writeAll(file, Flags.Append))
      .compile
      .drain

  def getCompactionFiles1(dir: Path): IO[CompactedFiles] =
    getCompactionFiles(dir).flatMap {
      case head :: Nil =>
        head.pure[IO]

      case Nil =>
        IO.raiseError(new Throwable("Missing Compaction Files"))

      case many =>
        IO.raiseError(
          new Throwable(many.map(file => s"Found extra ${file.keys} and ${file.values}").mkString("\n"))
        )
    }

  def getCompactionFiles(dir: Path): IO[List[CompactedFiles]] =
    attemptListCompactionFiles[IO](dir)
      .flatMap(_.traverse {
        case Left(value) =>
          failure(value).failFast[IO] *>
            IO.raiseError(new IllegalStateException("Not reachable, just for type sig"))

        case Right(files) =>
          files.pure[IO]
      })

}
