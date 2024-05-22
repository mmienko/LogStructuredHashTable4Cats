package io.lsht

import cats.Show
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.{Files, Flags, Path}
import io.lsht.CompactionFilesUtil.{attemptListCompactionFiles, writeKeyValueToCompactionFiles}
import io.lsht.TestUtils.{given_Show_Key, *}
import io.lsht.codec.{KeyValueCodec, TombstoneEncoder}
import weaver.*

import scala.concurrent.duration.*

object FileCompactionTest extends SimpleIOSuite {

  /*
  Directory state tests
   */
  test("Compaction on an empty directory does not perform compaction") {
    Files[IO].tempDirectory
      .use { dir =>
        FileCompaction.run[IO](dir) *> getCompactionFiles(dir)
      }
      .map(files => expect(files.isEmpty))
  }

  test("Compaction on an active datafile does not perform compaction") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.1.db")
        _ <- FileCompaction.run[IO](dir)
        files <- getCompactionFiles(dir)
      } yield expect(files.isEmpty)
    }
  }

  test("Compaction on an inactive datafile performs compaction") {
    Files[IO].tempDirectory.use { dir =>
      val `data.1.db` = dir / "data.1.db"
      for {
        _ <- Files[IO].createFile(`data.1.db`)
        _ <- appendEntriesToDataFile(`data.1.db`, KeyValue(Key("key"), "value".getBytes))
        _ <- Files[IO].createFile(dir / "data.2.db")
        _ <- FileCompaction.run[IO](dir)
        files <- getCompactionFiles1(dir)
        oldFileExists <- Files[IO].exists(`data.1.db`)
      } yield verify(!oldFileExists, hint = "Old Data File should be deleted")
    }
  }

  test("Compaction on an empty inactive datafiles does not perform compaction") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.1.db")
        _ <- Files[IO].createFile(dir / "data.2.db")
        _ <- FileCompaction.run[IO](dir)
        files <- getCompactionFiles(dir)
      } yield expect(files.isEmpty)
    }
  }

  test("Compaction on old compaction files does not perform compaction") {
    Files[IO].tempDirectory.use { dir =>
      val keysFile = dir / "keys.1.db"
      val valuesFile = dir / "values.1.db"
      for {
        _ <- Files[IO].createFile(keysFile)
        _ <- Files[IO].createFile(valuesFile)
        _ <- FileCompaction.run[IO](dir)
        files <- getCompactionFiles(dir)
      } yield matches(files) { case compactionFile :: Nil =>
        expect.all(
          compactionFile.timestamp === 1,
          compactionFile.keys === keysFile,
          compactionFile.values === valuesFile
        )
      }
    }
  }

  test(
    "Compaction on an active datafile and old compaction files does not perform compaction (compaction process was called before file rotation)"
  ) {
    Files[IO].tempDirectory.use { dir =>
      val keysFile = dir / "keys.1.db"
      val valuesFile = dir / "values.1.db"
      for {
        _ <- Files[IO].createFile(keysFile)
        _ <- Files[IO].createFile(valuesFile)
        _ <- Files[IO].createFile(dir / "data.1.db")
        _ <- FileCompaction.run[IO](dir)
        files <- getCompactionFiles(dir)
      } yield matches(files) { case compactionFile :: Nil =>
        expect.all(
          compactionFile.timestamp === 1,
          compactionFile.keys === keysFile,
          compactionFile.values === valuesFile
        )
      }
    }
  }

  test("Compaction on an inactive datafile and old compaction files performs compaction") {
    Files[IO].tempDirectory.use { dir =>
      val keysFile = dir / "keys.1.db"
      val valuesFile = dir / "values.1.db"
      for {
        _ <- writeToCompactionFile(
          dir,
          timestamp = 1.millis,
          KeyValue(Key("key1"), "value1".getBytes),
          KeyValue(Key("key2"), "value1".getBytes)
        )
        _ <- appendEntriesToDataFile(
          dir / "data.2.db",
          KeyValue(Key("key1"), "value2".getBytes),
          KeyValue(Key("key3"), "value2".getBytes)
        )
        _ <- Files[IO].createFile(dir / "data.3.db") // active file

        _ <- FileCompaction.run[IO](dir)

        compactionFiles <- getCompactionFiles1(dir)
        entries <- CompactionFilesUtil
          .readKeyValueEntries(compactionFiles)
          .collect { case Right(value) => value }
          .compile
          .toList
      } yield matches(entries) { case k1 :: k2 :: k3 :: Nil =>
        expect.all(
          k1 === KeyValue(Key("key1"), "value2".getBytes),
          k2 === KeyValue(Key("key2"), "value1".getBytes),
          k3 === KeyValue(Key("key3"), "value2".getBytes)
        )
      }
    }
  }

  test("Compaction on an inactive datafile and some unfinished compaction files performs compaction") { ignore("TODO") }

  test(
    "Compaction on an inactive datafile and some unfinished compaction files and 1 finished compaction files performs compaction"
  ) { ignore("TODO") }

  /*
  1. No data files : broken state - noop
  2. 1 Data file : active - noop
  3. Many Data files - active and inactive - 1 compaction files set, delete inactive files
  4. TODO: Compaction files set is missing a file : broken state - noop
  5. No data files & 1 Compaction file set : broken state - noop
  6. 1 data files & 1 Compaction file set : normal state (compaction process ran quicker than file rotation) - noop
  7. Many data files & 1 Compaction file set :
      a. normal state (file rotation ran quicker than compaction process) - new compaction files set, delete inactive files, delete old compaction file
      b. previous compaction did not finish - restart compaction like in step 3 + delete temp compaction files
  8. Many data files & Many Compaction file sets : broken state (since last valid/non-temporary compaction file) - new compaction file, deleted inactive files, deleted old compaction files
   */

  /*
  for 7b, write to temp location, then move file, then delete
   */

  /*
  TODO: New file?
  CompactionAlgorithm Tests
   */
  test("Compaction on a file with unique keys produces non-reduced compaction files") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.2.db") // active
        now <- IO.realTime.map(_.toMillis)

        _ <- appendEntriesToDataFile(
          file = dir / "data.1.db",
          keyValueEntries = (0 until 5).map(i => KeyValue(Key(s"key$i"), "value".getBytes))*
        )
        _ <- FileCompaction.run[IO](dir)

        files <- getCompactionFiles1(dir)
        entriesOrErrors <- CompactionFilesUtil.readKeyValueEntries[IO](files).compile.toList
      } yield expect(entriesOrErrors.length === 5) and
        forEach(entriesOrErrors.zipWithIndex) { case (entriesOrError, i) =>
          whenSuccess(entriesOrError) { kv =>
            expect(kv === KeyValue(Key(s"key$i"), "value".getBytes))
          }
        }
    }
  }

  test("Compaction on a file with duplicate keys produces reduced compaction files") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.2.db") // active
        now <- IO.realTime.map(_.toMillis)

        // TODO: can be moved to syntax file
        _ <- appendEntriesToDataFile(
          file = dir / "data.1.db",
          keyValueEntries = (0 until 5).flatMap { i =>
            List(
              KeyValue(Key(s"key$i"), "value0".getBytes),
              KeyValue(Key(s"key$i"), "value1".getBytes)
            )
          }*
        )
        _ <- FileCompaction.run[IO](dir)

        files <- getCompactionFiles1(dir)
        entriesOrErrors <- CompactionFilesUtil.readKeyValueEntries[IO](files).compile.toList
      } yield expect(entriesOrErrors.length === 5) and
        forEach(entriesOrErrors.zipWithIndex) { case (entriesOrError, i) =>
          whenSuccess(entriesOrError) { kv =>
            expect(kv === KeyValue(Key(s"key$i"), "value1".getBytes))
          }
        }
    }
  }

  test("Compaction on a file with deleted keys produces reduced compaction files") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.2.db") // active
        now <- IO.realTime.map(_.toMillis)

        // TODO: can be moved to syntax file
        _ <- appendEntriesToDataFile(
          file = dir / "data.1.db",
          keyValueEntries = (0 until 5).map(i => KeyValue(Key(s"key$i"), "value".getBytes))*
        )
        _ <- appendTombstonesToDataFile(
          file = dir / "data.1.db",
          tombstones = List(Key("key0"), Key("key2"), Key("key4"))*
        )
        _ <- FileCompaction.run[IO](dir)

        files <- getCompactionFiles1(dir)
        entriesOrErrors <- CompactionFilesUtil.readKeyValueEntries[IO](files).compile.toList
      } yield entriesOrErrors match
        case key1 :: key3 :: Nil =>
          whenSuccess(key1) { key =>
            expect(key === KeyValue(Key("key1"), "value".getBytes))
          } and whenSuccess(key3) { key =>
            expect(key === KeyValue(Key("key3"), "value".getBytes))
          }

        case _ =>
          failure(s"Expecting exactly 2 elements, instead got ${entriesOrErrors.length}")
    }
  }

  test("Compaction on a files with a distribution of keys produces reduced compaction files") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Files[IO].createFile(dir / "data.4.db") // active
        now <- IO.realTime.map(_.toMillis)

        dataFile1 = dir / "data.1.db"
        _ <- appendEntriesToDataFile(
          file = dataFile1,
          keyValueEntries = (0 until 5).flatMap { i =>
            List(
              KeyValue(Key(s"key$i"), "value0".getBytes),
              KeyValue(Key(s"key$i"), "value1".getBytes)
            )
          }*
        )
        _ <- appendTombstonesToDataFile(
          file = dataFile1,
          tombstones = List(Key("key0"), Key("key2"), Key("key4"))*
        )

        dataFile2 = dir / "data.2.db"
        _ <- appendEntriesToDataFile(
          file = dataFile2,
          keyValueEntries = (0 until 5).map(i => KeyValue(Key(s"key${i + 5}"), "value0".getBytes))*
        )

        dataFile3 = dir / "data.3.db"
        _ <- appendEntriesToDataFile(
          file = dataFile3,
          keyValueEntries = List(
            KeyValue(Key(s"key0"), "value2".getBytes),
            KeyValue(Key(s"key4"), "value2".getBytes), // adding back in
            KeyValue(Key(s"key5"), "value2".getBytes),
            KeyValue(Key(s"key9"), "value2".getBytes)
          )*
        )
        _ <- appendTombstonesToDataFile(
          file = dataFile3,
          tombstones = List(Key("key0"), Key("key2"), Key("key5"), Key("key7"))*
        )

        _ <- FileCompaction.run[IO](dir)

        files <- getCompactionFiles1(dir)
        entriesOrErrors <- CompactionFilesUtil.readKeyValueEntries[IO](files).compile.toList
      } yield entriesOrErrors.sortBy(_.fold(_ => "", _.key.show)) match
        case key1 :: key3 :: key4 :: key6 :: key8 :: key9 :: Nil =>
          whenSuccess(key1) { key =>
            expect.eql(KeyValue(Key("key1"), "value1".getBytes), key)
          } and whenSuccess(key3) { key =>
            expect.eql(KeyValue(Key("key3"), "value1".getBytes), key)
          } and whenSuccess(key4) { key =>
            expect.eql(KeyValue(Key("key4"), "value2".getBytes), key)
          } and whenSuccess(key6) { key =>
            expect.eql(KeyValue(Key("key6"), "value0".getBytes), key)
          } and whenSuccess(key8) { key =>
            expect.eql(KeyValue(Key("key8"), "value0".getBytes), key)
          } and whenSuccess(key9) { key =>
            expect.eql(KeyValue(Key("key9"), "value2".getBytes), key)
          }

        case _ =>
          failure(s"Expecting exactly 6 elements, instead got ${entriesOrErrors.length}")
    }

  }
}
