package io.lsht.engine

import cats.effect.*
import cats.syntax.all.*
import fs2.io.file.Files
import TestUtils.{DataFileNamePattern, expectSomeString, expectString}
import weaver.*

object FileRotationTest extends SimpleIOSuite {

  test("files are rotated") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir, entriesLimit = 3).use { db =>
          (db.put(Key("k1"), "v1".getBytes) *> db.delete(Key("k1")) *> db.put(Key("k1"), "v2".getBytes))
            .replicateA(3)
        }
        files <- Files[IO].list(dir).compile.toList
        _ <- expect(files.size === 3).failFast
        fileNames = files.map(_.fileName.toString)
      } yield expect(fileNames.toSet.size === 3) and
        forEach(fileNames)(fn => expect(fn.matches(DataFileNamePattern)))
    }
  }

  test("data from previous data files is loaded") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir, entriesLimit = 3).use { db =>
          (0 to 3).toList.traverse_ { i =>
            val key = Key(s"k$i")
            db.put(key, "v1".getBytes) *> db.delete(key) *> db.put(key, "v2".getBytes)
          }
        }
        gets <- Database[IO](dir, entriesLimit = 3).use { db =>
          (0 until 3).toList.traverse(i => db.get(Key(s"k$i")))
        }
        values = gets.collect { case Some(v) => v }
      } yield expect(values.size === 3) and
        forEach(values)(expectString("v2"))
    }
  }

  test("most recent writes take precedence over previous writes in old files") {
    Files[IO].tempDirectory.use { dir =>
      val key1 = Key("k1")
      val key2 = Key("k2")
      val key3 = Key("k3")
      val key4 = Key("k4")
      for {
        _ <- Database[IO](dir, entriesLimit = 3).use { db =>
          db.put(key1, "v1".getBytes) *> db.put(key2, "v1".getBytes) *> db.put(key3, "v1".getBytes) *>
            db.put(key4, "v1".getBytes) *> db.put(key1, "v2".getBytes) *> db.delete(key2) *>
            db.put(key4, "v2".getBytes) *> db.put(key1, "v3".getBytes)
        }
        _ <- Database[IO](dir, entriesLimit = 3).use { db =>
          db.get(key1).map(expectSomeString("v3")).flatMap(_.failFast) *>
            db.get(key2).map(res => expect(res.==(none[Value]))).flatMap(_.failFast) *>
            db.get(key3).map(expectSomeString("v1")).flatMap(_.failFast) *>
            db.get(key4).map(expectSomeString("v2")).flatMap(_.failFast)
        }
      } yield success
    }
  }

  test("blank files don't break loading database") {
    Files[IO].tempDirectory.use { dir =>
      for {
        _ <- Database[IO](dir, entriesLimit = 3).use { db =>
          db.put(Key("k1"), "v1".getBytes) *> db.delete(Key("k1")) *> db.put(Key("k1"), "v2".getBytes)
        }
        _ <- Database[IO](dir, entriesLimit = 3).use_
      } yield success
    }
  }
}
