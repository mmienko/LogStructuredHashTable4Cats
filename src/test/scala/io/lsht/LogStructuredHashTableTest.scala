package io.lsht

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.*
import io.lsht.TestUtils.DataFileNamePattern
import weaver.*

import java.nio.file.AccessDeniedException
import java.util.regex.Pattern

object LogStructuredHashTableTest extends SimpleIOSuite {

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

  test("Database can be re-opened") {
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
      .flatMap(LogStructuredHashTable[IO](_))
      .use { db =>
        val key = Key("key".getBytes)
        val value = "value".getBytes
        for {
          res <- db.get(key)
          _ <- expect(res.isEmpty).failFast
          _ <- db.put(key, value)
          res <- db.get(key)
        } yield exists(res)(v => expect(new String(v) === "value"))
      }
  }

  test("Write is persisted across open & close of Database") {
    val key = Key("key".getBytes)
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
      .flatMap(LogStructuredHashTable[IO](_))
      .use { db =>
        for {
          uuids <- IO.randomUUID.replicateA(20)
          ids = uuids.map(_.toString.getBytes).map(Key.apply)
          gets <- ids.parTraverse(db.get)
          _ <- gets
            .map(res => expect(res.isEmpty))
            .reduce((a, b) => a and b)
            .failFast
          _ <- ids.parTraverse(id => db.put(id, id.value))
          gets <- ids.parTraverse(db.get)
          values = gets.collect { case Some(v) => new String(v) }.toSet
        } yield expect(values === uuids.map(_.toString).toSet)
      }
  }

  test("Database supports reads and write over multiple reopens") {
    val valuesCardinality = 20
    Files[IO].tempDirectory.use { dir =>
      for {
        uuids <- IO.randomUUID.replicateA(100)
        ids = uuids.map(_.toString.getBytes).map(Key.apply)
        _ <- fs2.Stream
          .evals(ids.pure[IO])
          .chunkN(valuesCardinality)
          .evalMap { ids =>
            LogStructuredHashTable[IO](dir).use { db =>
              ids.zipWithIndex.parTraverse_ { case (key, i) => db.put(key, s"value$i".getBytes) }
            }
          }
          .compile
          .drain
        gets <- LogStructuredHashTable[IO](dir)
          .use(db => ids.parTraverse(db.get))
        values = gets.collect { case Some(v) => new String(v) }.toSet
      } yield expect(values === Set.tabulate(valuesCardinality)(i => "value" + i))
    }
  }

  test("Database supports overwriting keys") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO](_))
      .use { db =>
        val key = Key("key".getBytes)
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
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = Key("key".getBytes)
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // Corrupt value
          dataFile <- Files[IO].list(dir).compile.lastOrError
          bytes <- Files[IO].readAll(dataFile).compile.to(Array)
          bytes <- IO(bytes.updated(bytes.length - 1, 1.toByte))
          _ <- Files[IO]
            .writeCursor(dataFile, Flags.Write)
            .use(_.write(Chunk.array(bytes)))
          // Read corrupted value
          res <- db.get(key).attempt
        } yield matches(res) { case Left(err) =>
          expect(err == Errors.Read.BadChecksum)
        }
      }
    }
  }

  test(
    "Database reports an error if entry was never fully written to file (mimic a crash)"
  ) {
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = Key("key".getBytes)
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // trim entry
          dataFile <- Files[IO].list(dir).compile.lastOrError
          _ <- Files[IO].open(dataFile, Flags.Write).use(_.truncate(6))
          // Read corrupted value
          res <- db.get(key).attempt
        } yield matches(res) { case Left(err) =>
          expect(err == Errors.Read.CorruptedDataFile)
        }
      }
    }
  }

  test("Database reports an error if file is empty") {
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = Key("key".getBytes)
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // Corrupt value
          dataFile <- Files[IO].list(dir).compile.lastOrError
          _ <- Files[IO].open(dataFile, Flags.Write).use(_.truncate(0))
          // Read corrupted value
          res <- db.get(key).attempt
        } yield matches(res) { case Left(err) =>
          expect(err == Errors.Read.CorruptedDataFile)
        }
      }
    }
  }

  test("Database reports an error if file is missing") {
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = Key("key".getBytes)
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // Corrupt value
          dataFile <- Files[IO].list(dir).compile.lastOrError
          _ <- Files[IO].delete(dataFile)
          // Read corrupted value
          res <- db.get(key).attempt
        } yield matches(res) { case Left(Errors.Read.FileSystem(err: NoSuchFileException)) =>
          expect(err.getFile === dataFile.toString)
        }
      }
    }
  }

  test("Database reports an error if file permissions changed") {
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).use { db =>
        val key = Key("key".getBytes)
        for {
          // Write initial value
          _ <- db.put(key, "value".getBytes)
          // Change permissions
          dataFile <- Files[IO].list(dir).compile.lastOrError
          _ <- Files[IO].setPosixPermissions(
            dataFile,
            PosixPermissions(PosixPermission.OwnerWrite)
          )
          // Read
          res <- db.get(key).attempt
        } yield matches(res) { case Left(Errors.Read.FileSystem(err: AccessDeniedException)) =>
          expect(err.getFile === dataFile.toString)
        }
      }
    }
  }

  test("Database Resource leak can be detected") {
    Files[IO].tempDirectory.use { dir =>
      LogStructuredHashTable[IO](dir).allocated
        .flatMap { case (db, close) => close.as(db) }
        .flatMap(db => db.put(Key("key"), "value".getBytes).attempt)
        .map(res =>
          matches(res) { case Left(t: IllegalStateException) =>
            expect(t.getMessage === "Resource leak, db is closed and this method should not be called")
          }
        )
    }
  }

  test("Database has no effect when deleting a key that doesn't exist") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO](_))
      .use { db =>
        db.delete(Key("key1"))
      }
      .as(success)
  }

  test("Database can delete existing keys") {
    Files[IO].tempDirectory
      .flatMap(LogStructuredHashTable[IO](_))
      .use { db =>
        val key = Key("key1")
        db.put(key, "value1".getBytes) *>
          db.delete(key) *>
          db.get(key)
      }
      .map(result => expect(result.isEmpty))
  }

  test("Database does not load deleted keys") {
    Files[IO].tempDirectory
      .use { dir =>
        val key1 = Key("key1")
        val key2 = Key("key2")
        LogStructuredHashTable[IO](dir).use { db =>
          db.put(key1, "value1".getBytes) *>
            db.put(key2, "value2".getBytes) *>
            db.delete(key1)
        } *> LogStructuredHashTable[IO](dir)
          .use { db =>
            db.get(key1)
              .flatMap(res1 => db.get(key2).tupleLeft(res1))
              .flatTap(_ => db.delete(key2))
          }
          .flatMap { case (res1, res2) =>
            (expect(res1.isEmpty) and matches(res2) { case Some(value) =>
              expect(new String(value) === "value2")
            }).failFast
          } *> LogStructuredHashTable[IO](dir)
          .use { db =>
            db.get(key2)
          }
          .map(res => expect(res.isEmpty))
      }
  }
}
