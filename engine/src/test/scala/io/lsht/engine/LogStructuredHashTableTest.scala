package io.lsht.engine

import cats.effect.*
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.file.*
import TestUtils.{StringKeyOrdering, given_Show_Array, writeToCompactionFile, *}
import Value.equality
import weaver.*

import scala.concurrent.duration.*

object LogStructuredHashTableTest extends SimpleIOSuite {

  test("Database can be opened and closed") {
    Files[IO].tempDirectory.use { dir =>
      for {
        files <- Files[IO].list(dir).compile.toList
        _ <- expect(files.isEmpty).failFast
        _ <- Database[IO](dir).use_
        files <- Files[IO].list(dir).compile.toList
      } yield expect(files.isEmpty)
    }
  }

  test("Database can be re-opened") {
    Files[IO].tempDirectory
      .use { dir =>
        for {
          _ <- Database[IO](dir).use_
          _ <- Database[IO](dir).use_
          files <- Files[IO].list(dir).compile.toList
        } yield expect(files.isEmpty)
      }
  }

  test("Database supports reads and writes") {
    tempDatabase.use { db =>
      val key = Key("key")
      val value = "value".getBytes
      for {
        res <- db.get(key)
        _ <- expect(res.isEmpty).failFast
        _ <- db.put(key, value)
        res <- db.get(key)
      } yield exists(res)(expectString("value"))
    }
  }

  test("Write is persisted across open & close of Database") {
    val key = Key("key")
    val value = "value".getBytes
    Files[IO].tempDirectory
      .use { dir =>
        for {
          _ <- Database[IO](dir).use(_.put(key, value))
          res <- Database[IO](dir).use(_.get(key))
        } yield exists(res)(expectString("value"))
      }
  }

  test("Database supports multiple reads and writes") {
    tempDatabase.use { db =>
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
      } yield expect.eql(values, uuids.map(_.toString).toSet)
    }
  }

  test("Database supports overwriting keys") {
    tempDatabase.use { db =>
      val key = Key("key")
      val value1 = "value1".getBytes
      val value2 = "value2".getBytes
      for {
        _ <- db.put(key, value1)
        _ <- db.put(key, value2)
        res <- db.get(key)
      } yield exists(res)(expectString("value2"))
    }
  }

  test("Database supports writes over multiple reopens") {
    val valuesCardinality = 20
    Files[IO].tempDirectory.use { dir =>
      for {
        uuids <- IO.randomUUID.replicateA(100)
        keys = uuids.map(_.toString.getBytes).map(Key.apply)
        _ <- fs2.Stream
          .evals(keys.pure[IO])
          .chunkN(valuesCardinality)
          .evalMap { keys =>
            Database[IO](dir).use { db =>
              keys.zipWithIndex.parTraverse_ { case (key, i) => db.put(key, s"value$i".getBytes) }
            }
          }
          .compile
          .drain
        gets <- Database[IO](dir)
          .use(db => keys.parTraverse(db.get))
        values = gets.collect { case Some(v) => new String(v) }.toSet
      } yield expect.eql(values, Set.tabulate(valuesCardinality)(i => "value" + i))
    }
  }

  test("Database has no effect when deleting a key that doesn't exist") {
    tempDatabase
      .use { db => db.delete(Key("key1")) }
      .as(success)
  }

  test("Database can delete existing keys") {
    tempDatabase
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
        Database[IO](dir).use { db =>
          db.put(key1, "value1".getBytes) *>
            db.put(key2, "value2".getBytes) *>
            db.delete(key1)
        } *> Database[IO](dir)
          .use { db =>
            db.get(key1)
              .flatMap(res1 => db.get(key2).tupleLeft(res1))
              .flatTap(_ => db.delete(key2))
          }
          .flatMap { case (res1, res2) =>
            (expect(res1.isEmpty) and exists(res2)(expectString("value2"))).failFast
          } *> Database[IO](dir)
          .use { db =>
            db.get(key2)
          }
          .map(res => expect(res.isEmpty))
      }
  }

  test("`keys` operation retrieve all keys") {
    tempDatabase.use { db =>
      for {
        _ <- putAll(db)(KeyValue("k1", "v1"), KeyValue("k2", "v2"), KeyValue("k3", "v3"))

        keys <- db.keys.compile.toVector
        _ <- expect
          .eql(
            keys.sorted(StringKeyOrdering),
            Vector(Key("k1"), Key("k2"), Key("k3"))
          )
          .failFast

        _ <- db.delete(Key("k2"))

        keys <- db.keys.compile.toVector
      } yield expect.eql(
        keys.sorted(StringKeyOrdering),
        Vector(Key("k1"), Key("k3"))
      )
    }
  }

  test("Database can read keys from compacted files") {
    tempDirectory.use { dir =>
      for {
        _ <- writeToCompactionFile(dir, timestamp = 1.millis, KeyValue("key1", "value1"))
        value <- Database[IO](dir).use(_.get(Key("key1")))
        _ <- expect.eql(Value("value1").some, value).failFast
        _ <- Database[IO](dir).use(_.delete(Key("key1")))
        value <- Database[IO](dir).use(_.get(Key("key1")))
      } yield expect(value.isEmpty)
    }
  }

  test("Database takes latest keys from data files over compacted files") {
    tempDirectory.use { dir =>
      for {
        _ <- writeToCompactionFile(dir, timestamp = 1.millis, KeyValue("key1", "value1"), KeyValue("key2", "value1"))
        _ <- appendEntriesToDataFile(dir / "data.1.db", KeyValue("key2", "value2"), KeyValue("key3", "value1"))
        value <- Database[IO](dir).use(_.get(Key("key1")))
        _ <- expect.eql(Value("value1").some, value).failFast
        value <- Database[IO](dir).use(_.get(Key("key2")))
        _ <- expect.eql(Value("value2").some, value).failFast
        value <- Database[IO](dir).use(_.get(Key("key3")))
      } yield expect.eql(Value("value1").some, value)
    }
  }
}
