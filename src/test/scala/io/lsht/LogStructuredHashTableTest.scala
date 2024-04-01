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

  test("Database supports reads and writes") {
    (for {
      dir <- Files[IO].tempDirectory
      db <- LogStructuredHashTable[IO](dir)
    } yield db).use { db =>
      val key = "key".getBytes
      val value: Value = "value".getBytes
      for {
        res <- db.get(key)
        _ <- expect(res.isEmpty).failFast
        _ <- db.put(key, value)
        res <- db.get(key)
      } yield exists(res)(v => expect(new String(v) === "value"))
    }
  }

}
