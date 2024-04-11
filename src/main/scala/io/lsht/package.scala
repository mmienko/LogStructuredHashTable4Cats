package io.lsht

import cats.effect.{Deferred, GenConcurrent}
import cats.syntax.all.*

import java.util

final case class Key(value: Array[Byte]) extends AnyVal {
  def length: Int = value.length

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[Key] && obj.asInstanceOf[Key].value.sameElements(value)

  override def hashCode(): Int = util.Arrays.hashCode(value)
}

object Key {
  def apply(string: String): Key = new Key(string.getBytes)
}

type Value = Array[Byte]

private type WriteResult = Unit | Throwable

private final case class KeyValueEntry(key: Key, value: Value) {
  def size: Int = key.length + value.length
}

private sealed abstract class WriteCommand[F[_]](signal: Deferred[F, WriteResult]) extends Product with Serializable {
  def waitUntilComplete: F[WriteResult] = signal.get

  def complete(res: WriteResult): F[Boolean] = signal.complete(res)
}

object WriteCommand {
  final case class Put[F[_]](
      keyValueEntry: KeyValueEntry,
      signal: Deferred[F, WriteResult]
  ) extends WriteCommand[F](signal) {
    def key: Key = keyValueEntry.key
    def value: Value = keyValueEntry.value
    def entrySize: Int = keyValueEntry.size
  }

  object Put {
    def apply[F[_]](key: Key, value: Value)(implicit F: GenConcurrent[F, ?]): F[Put[F]] =
      Deferred[F, WriteResult].map(Put(KeyValueEntry(key, value), _))
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
