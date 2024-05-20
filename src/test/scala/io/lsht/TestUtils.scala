package io.lsht

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import weaver.{Expect, Expectations}

object TestUtils {
  val DataFileNamePattern: String = "data\\.\\d*\\.db"

  given StringKeyOrdering: Ordering[Key] = Ordering.String.contramap[Key](k => new String(k.value))

  def expectSomeString(expected: String)(actual: Option[Array[Byte]]): Expectations =
    (new Expect)(actual.map(new String(_)) === expected.some)

  def expectString(expected: String)(actual: Array[Byte]): Expectations =
    (new Expect)(new String(actual) === expected)

  def tempDatabase: Resource[IO, LogStructuredHashTable[IO]] =
    Files[IO].tempDirectory.flatMap(Database[IO](_))

  def tempDatabaseWithDir: Resource[IO, (LogStructuredHashTable[IO], Path)] =
    Files[IO].tempDirectory.flatMap(dir => Database[IO](dir).tupleRight(dir))

  def putAll(db: LogStructuredHashTable[IO])(keyValuePairs: KeyValue*): IO[Unit] =
    keyValuePairs.traverse_(kv => db.put(kv.key, kv.value))
}
