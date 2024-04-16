package io.lsht

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import fs2.io.file.Files
import weaver.{Expect, Expectations}

object TestUtils {
  val DataFileNamePattern: String = "data\\.\\d*\\.db"

  // TODO: expect string
  def expectSomeString(expected: String)(actual: Option[Array[Byte]]): Expectations =
    (new Expect)(actual.map(new String(_)) === expected.some)

  def expectString(expected: String)(actual: Array[Byte]): Expectations =
    (new Expect)(new String(actual) === expected)

  // TODO: use this in tests
  def tempDatabase: Resource[IO, LogStructuredHashTable[IO]] =
    Files[IO].tempDirectory.flatMap(LogStructuredHashTable[IO](_))
}
