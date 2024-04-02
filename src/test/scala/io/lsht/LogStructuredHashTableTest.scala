package io.lsht

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.{Files, Flags, Path}
import weaver.*

object LogStructuredHashTableTest extends SimpleIOSuite {
  private val DataFileNamePattern = "data\\.\\d*\\.db"

  test("Database can be opened and closed") {
    (for {
      dir <- Files[IO].tempDirectory
      files <- Resource.eval(Files[IO].list(dir).compile.toList)
      _ <- Resource.eval(expect(files.isEmpty).failFast)
      _ <- LogStructuredHashTable[IO](dir)
      files <- Resource.eval(Files[IO].list(dir).compile.toList)
      _ <- Resource.eval(expect(files.size === 1).failFast)
    } yield {
      expect(files.head.fileName.toString.matches(DataFileNamePattern))
    }).use(IO.pure)
  }

  test("Database can be re-opened".ignore) {
    Files[IO].tempDirectory
      .use { dir =>
        LogStructuredHashTable[IO](dir).use_ *>
          LogStructuredHashTable[IO](dir).use_ *>
          Files[IO].list(dir).compile.toList
      }
      .map { files =>
        expect(files.size === 1) and
          expect(files.head.fileName.toString.matches(DataFileNamePattern))
      }
  }

  test("Database supports reads and writes") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO])
      .use { db =>
        val key = "key".getBytes
        val value = "value".getBytes
        for {
          res <- db.get(key)
          _ <- expect(res.isEmpty).failFast
          _ <- db.put(key, value)
          res <- db.get(key)
        } yield exists(res)(v => expect(new String(v) === "value"))
      }
  }

  test("Writes are persisted across open & close of Database".ignore) {
    val key = "key".getBytes
    val value = "value".getBytes
    Files[IO].tempDirectory
      .use { dir =>
        LogStructuredHashTable[IO](dir).use(_.put(key, value)) *>
          LogStructuredHashTable[IO](dir).use(_.get(key))
      }
      .map(res => exists(res)(v => expect(new String(v) === "value")))
  }

  test("Database supports multiple reads and writes") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO])
      .use { db =>
        val value = "value".getBytes
        for {
          uuids <- IO.randomUUID.replicateA(100)
          ids = uuids.map(_.toString.getBytes)
          gets <- ids.parTraverse(db.get)
          _ <- gets
            .map(res => expect(res.isEmpty))
            .reduce((a, b) => a and b)
            .failFast
          _ <- ids.parTraverse(id => db.put(id, value))
          gets <- ids.parTraverse(db.get)
        } yield
          gets
            .map(res => exists(res)(v => expect(new String(v) === "value")))
            .reduce((a, b) => a and b)
      }
  }

  test("Database supports reads and write over multiple reopens".ignore) {
    Files[IO].tempDirectory.use { dir =>
      val value = "value".getBytes
      for {
        uuids <- IO.randomUUID.replicateA(100)
        ids = uuids.map(_.toString.getBytes)
        _ <- fs2.Stream
          .evals(ids.pure[IO])
          .chunkN(20)
          .evalMap { ids =>
            LogStructuredHashTable[IO](dir).use(
              db => ids.parTraverse_(db.put(_, value))
            )
          }
          .compile
          .drain
        gets <- LogStructuredHashTable[IO](dir)
          .use(db => ids.parTraverse(db.get))
      } yield
        gets
          .map(res => exists(res)(v => expect(new String(v) === "value")))
          .reduce((a, b) => a and b)
    }
  }

  test("Database supports overwriting keys") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO])
      .use { db =>
        val key = "key".getBytes
        val value1 = "value1".getBytes
        val value2 = "value2".getBytes
        for {
          _ <- db.put(key, value1)
          _ <- db.put(key, value2)
          res <- db.get(key)
        } yield exists(res)(v => expect(new String(v) === "value2"))
      }
  }

  test("Database validates checksum") {
    // TODO: does it make sense to use different Checksums depending on the length of data? Is there different performance?
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = "key".getBytes
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // Corrupt value
          dataFile <- Files[IO].list(dir).compile.lastOrError
          bytes <- Files[IO].readAll(dataFile).compile.to(Array)
          bytes <- IO(bytes.updated(bytes.length - 1, 1.toByte))
          // Read corrupted value
          _ <- Files[IO]
            .writeCursor(dataFile, Flags.Write)
            .use(_.write(Chunk.array(bytes)))
          res <- db.get(key).attempt
        } yield expect(res.isLeft)
      }
    }
  }

}
