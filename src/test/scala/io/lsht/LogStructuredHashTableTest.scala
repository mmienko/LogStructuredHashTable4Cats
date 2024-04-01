package io.lsht

import cats.effect.*
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import weaver.*

object LogStructuredHashTableTest extends SimpleIOSuite {

  test("Database can be opened and closed") {
    (for {
      dir <- Files[IO].tempDirectory
      _ <- LogStructuredHashTable[IO](dir)
      files <- Resource.eval(Files[IO].list(dir).compile.toList)
      _ <- Resource.eval(expect(files.size === 1).failFast)
    } yield expect(files.head.fileName.toString.matches("data\\.\\d*\\.db")))
      .use(IO.pure)
  }

}
