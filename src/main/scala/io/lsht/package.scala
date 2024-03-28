package io.lsht

import cats.effect.Deferred

type Key = Array[Byte]
type Value = Array[Byte]

private type PutResult = Unit | Throwable

private final case class Put[F[_]](
    key: Key,
    value: Value,
    signal: Deferred[F, PutResult]
) {
  def dataSize: Int = key.length + value.length
}
