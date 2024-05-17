package io.lsht

import cats.Eq
import cats.effect.{Deferred, GenConcurrent}
import cats.syntax.all.*
import fs2.io.file.Path
import Value.equality

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

object Value {
  given equality: Eq[Array[Byte]] = Eq.instance(_ sameElements _)
}

private final case class KeyValue(key: Key, value: Value) {
  def size: Int = key.length + value.length
}

object KeyValue {
  given Eq[KeyValue] = Eq.and(Eq.by(_.key), Eq.by(_.value))
}

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

private type Offset = Long

/**
  * Serialized KeyValue record in a DataFile
  * @param file
  *   Datafile path
  * @param offset
  *   position in the file
  * @param length
  *   length of serialized KeyValue record (header data included)
  */
private final case class KeyValueFileReference(file: Path, offset: Offset, length: Int)

object KeyValueFileReference {
  given Eq[KeyValueFileReference] = Eq.instance { (a, b) =>
    a.file === b.file && a.offset == b.offset && a.length === b.length
  }
}

private given Ordering[Path] = (x: Path, y: Path) => x.fileName.toString.compare(y.fileName.toString)

private final case class CompactedValue(offset: Offset, length: Int)
private final case class CompactedKey(key: Key, compactedValue: CompactedValue)

private object CompactedKey {
  given Eq[CompactedKey] = Eq.fromUniversalEquals
}

private final case class CompactedFiles(keys: Path, values: Path, timestamp: Long)
