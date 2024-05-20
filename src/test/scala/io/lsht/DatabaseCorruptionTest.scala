package io.lsht

import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.*
import io.lsht.LogStructuredHashTableTest.{expect, matches, test}
import io.lsht.TestUtils.tempDatabaseWithDir
import io.lsht.Value.equality
import io.lsht.codec.KeyValueCodec
import weaver.*

import java.nio.file.AccessDeniedException

object DatabaseCorruptionTest extends SimpleIOSuite {

  test("Database doesn't load latest record if header is incomplete") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir).use(_.put(Key("key1"), Value("value")))

        dataFile <- Files[IO].list(dir).compile.lastOrError
        bb <- KeyValueCodec.encode(KeyValue("key2", "value"))
        incompleteHeader = bb.array().take(KeyValueCodec.HeaderSize - 2)
        _ <- writeToDataFile(incompleteHeader, dir)

        x <- Database[IO](dir).use { db =>
          for {
            v1 <- db.get(Key("key1"))
            v2 <- db.get(Key("key2"))
            keys <- db.keys.compile.toList
          } yield (v1, v2, keys)
        }
        (v1, v2, keys) = x
      } yield expect.all(
        v1 === Value("value").some,
        v2 === None,
        keys === List(Key("key1"))
      )
    }
  }

  test("Database doesn't load latest record if data is missing") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir).use(_.put(Key("key1"), Value("value")))

        dataFile <- Files[IO].list(dir).compile.lastOrError
        bb <- KeyValueCodec.encode(KeyValue("key2", "value"))
        justHeader = bb.array().take(KeyValueCodec.HeaderSize)
        _ <- writeToDataFile(justHeader, dir)

        x <- Database[IO](dir).use { db =>
          for {
            v1 <- db.get(Key("key1"))
            v2 <- db.get(Key("key2"))
            keys <- db.keys.compile.toList
          } yield (v1, v2, keys)
        }
        (v1, v2, keys) = x
      } yield expect.all(
        v1 === Value("value").some,
        v2 === None,
        keys === List(Key("key1"))
      )
    }
  }

  test("Database doesn't load latest record if data is corrupted") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir).use(_.put(Key("key1"), Value("value")))

        dataFile <- Files[IO].list(dir).compile.lastOrError
        bb <- KeyValueCodec.encode(KeyValue("key2", "value"))
        corrupted = bb.array().updated(KeyValueCodec.HeaderSize + 1, 1.toByte)
        _ <- writeToDataFile(corrupted, dir)

        x <- Database[IO](dir).use { db =>
          for {
            v1 <- db.get(Key("key1"))
            v2 <- db.get(Key("key2"))
            keys <- db.keys.compile.toList
          } yield (v1, v2, keys)
        }
        (v1, v2, keys) = x
      } yield expect.all(
        v1 === Value("value").some,
        v2 === None,
        keys === List(Key("key1"))
      )
    }
  }

  test("Database loads remaining records even if data is corrupted, but errors on `get`") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir).use(_.put(Key("key1"), Value("value")))
        dataFile <- Files[IO].list(dir).compile.lastOrError
        bb <- KeyValueCodec.encode(KeyValue("key2", "value"))
        corrupted = bb.array().updated(KeyValueCodec.HeaderSize + 1, 1.toByte)
        _ <- writeToDataFile(corrupted, dir)
        _ <- Database[IO](dir).use(_.put(Key("key3"), Value("value")))

        x <- Database[IO](dir).use { db =>
          for {
            v1 <- db.get(Key("key1"))
            v2 <- db.get(Key("key2"))
            v3 <- db.get(Key("key3"))
            keys <- db.keys.compile.toList
//            check <- db.checkIntegrity(Key("key2"))
          } yield (v1, v2, v3, keys)
        }
        (v1, v2, v3, keys) = x
      } yield expect.all(
        v1 === Value("value").some,
        v2 === None,
        v3 === Value("value").some,
        keys === List(Key("key1"), Key("key3"))
//        check == Errors.Read.BadChecksum
      )
    }
  }

  // TODO: could repeat tests for delete commands and compaction

  test("Database reports an error if file is empty") {
    tempDatabaseWithDir.use { (db, dir) =>
      val key = Key("key")
      for {
        // Write initial value
        _ <- db.put(key, "value".getBytes)
        // Corrupt value
        dataFile <- Files[IO].list(dir).compile.lastOrError
        _ <- Files[IO].open(dataFile, Flags.Write).use(_.truncate(0))
        // Read corrupted value
        res <- db.get(key)
        _ <- expect.eql(res, None).failFast
        integrity <- db.checkIntegrity(key)
      } yield matches(integrity) { case Some(err) =>
        expect(err == Errors.Read.CorruptedDataFile)
      }
    }
  }

  test("Database reports an error if file is missing") {
    tempDatabaseWithDir.use { (db, dir) =>
      val key = Key("key")
      for {
        // Write initial value
        _ <- db.put(key, "value".getBytes)
        // Corrupt value
        dataFile <- Files[IO].list(dir).compile.lastOrError
        _ <- Files[IO].delete(dataFile)
        // Read corrupted value
        res <- db.get(key)
        _ <- expect.eql(res, None).failFast
        integrity <- db.checkIntegrity(key)
      } yield matches(integrity) { case Some(Errors.Read.FileSystem(err: NoSuchFileException)) =>
        expect.eql(err.getFile, dataFile.toString)
      }
    }
  }

  test("Database reports an error if file permissions changed") {
    tempDatabaseWithDir.use { (db, dir) =>
      val key = Key("key")
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
        res <- db.get(key)
        _ <- expect.eql(res, None).failFast
        integrity <- db.checkIntegrity(key)
      } yield matches(integrity) { case Some(Errors.Read.FileSystem(err: AccessDeniedException)) =>
        expect.eql(err.getFile, dataFile.toString)
      }
    }
  }

  test("Database Resource leak can be detected") {
    Files[IO].tempDirectory.use { dir =>
      for {
        db <- Database(dir).use(_.pure[IO])
        res <- db.put(Key("key"), "value".getBytes).attempt
      } yield matches(res) { case Left(t: IllegalStateException) =>
        expect.eql(t.getMessage, "Resource leak, db is closed and this method should not be called")
      }
    }
  }

  private def write(dataFile: Path, bytes: Array[Byte]) =
    Files[IO]
      .writeCursor(dataFile, Flags.Append)
      .use(_.write(Chunk.array(bytes)))

  private def writeToDataFile(bytes: Array[Byte], dir: Path) =
    Files[IO]
      .list(dir)
      .compile
      .lastOrError
      .flatMap(write(_, bytes))

}
