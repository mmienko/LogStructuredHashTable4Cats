package io.lsht

import cats.Eq
import cats.effect.{Deferred, GenConcurrent}
import cats.syntax.all.*
import fs2.io.file.Path

import java.util

final case class Key(value: Array[Byte]) extends AnyVal {
  def length: Int = value.length

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[Key] && obj.asInstanceOf[Key].value.sameElements(value)

  override def hashCode(): Int = util.Arrays.hashCode(value)
}

object Key {

  given Eq[Key] = Eq.by(_.value)

  def apply(string: String): Key = new Key(string.getBytes)
}

type Value = Array[Byte]

private final case class KeyValue(key: Key, value: Value) {
  def size: Int = key.length + value.length
}

object KeyValue {
  given Eq[KeyValue] = Eq.and(Eq.by(_.key), Eq.by(_.value))
}

private type Offset = Long

private type WriteResult = Unit | Throwable

private sealed abstract class WriteCommand[F[_]](signal: Deferred[F, WriteResult]) extends Product with Serializable {
  def waitUntilComplete: F[WriteResult] = signal.get

  def complete(res: WriteResult): F[Boolean] = signal.complete(res)
}

object WriteCommand {
  final case class Put[F[_]](
      keyValue: KeyValue,
      signal: Deferred[F, WriteResult]
  ) extends WriteCommand[F](signal) {
    def key: Key = keyValue.key
    def value: Value = keyValue.value
    def entrySize: Int = keyValue.size
  }

  object Put {
    def apply[F[_]](key: Key, value: Value)(implicit F: GenConcurrent[F, ?]): F[Put[F]] =
      Deferred[F, WriteResult].map(Put(KeyValue(key, value), _))
  }

  final case class Delete[F[_]](
      key: Key,
      signal: Deferred[F, WriteResult]
  ) extends WriteCommand[F](signal)

  object Delete {
    def apply[F[_]](key: Key)(implicit F: GenConcurrent[F, ?]): F[Delete[F]] =
      Deferred[F, WriteResult].map(Delete(key, _))
  }

}

private final case class KeyValueFileReference(filePath: Path, positionInFile: Long, entrySize: Int)

object KeyValueFileReference {
  given Eq[KeyValueFileReference] = Eq.instance { (a, b) =>
    a.filePath === b.filePath && a.positionInFile == b.positionInFile && a.entrySize === b.entrySize
  }
}

// TODO: use in DB
private given Ordering[Path] = (x: Path, y: Path) => x.fileName.toString.compare(y.fileName.toString)
// TODO: Eq[Value]? under Value type
private given Eq[Array[Byte]] = Eq.instance(_ sameElements _)

private final case class EntryHint(key: Key, positionInFile: Long, valueSize: Int)

private object EntryHint {
  given Eq[EntryHint] = Eq.fromUniversalEquals
}

private final case class CompactedFiles(hint: Path, values: Path, timestamp: Long)
