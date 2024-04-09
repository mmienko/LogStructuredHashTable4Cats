package io.lsht

import cats.effect.{Deferred, GenConcurrent}
import cats.syntax.all.*

case class Key(value: Array[Byte]) extends AnyVal {
  def length: Int = value.length
  override def equals(obj: Any): Boolean =
    canEqual(obj) || (obj.isInstanceOf[Array[Byte]] && obj.asInstanceOf[Array[Byte]].sameElements(value))
}

type Value = Array[Byte]

private type PutResult = Unit | Throwable

private final case class Put(key: Key, value: Value) {
  def dataSize: Int = key.length + value.length
}

private final case class PutCommand[F[_]](
    put: Put,
    signal: Deferred[F, PutResult]
) {
  def key: Key = put.key
  def value: Value = put.value
  def dataSize: Int = put.dataSize
  def waitUntilComplete: F[PutResult] = signal.get
  def complete(res: PutResult): F[Boolean] = signal.complete(res)
}

object PutCommand {
  def apply[F[_]](key: Key, value: Value)(implicit F: GenConcurrent[F, ?]): F[PutCommand[F]] =
    Deferred[F, PutResult].map(PutCommand(Put(key, value), _))
}
