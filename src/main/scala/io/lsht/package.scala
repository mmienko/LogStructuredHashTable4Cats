package io.lsht

import cats.effect.Deferred

type Key = Array[Byte]
type Value = Array[Byte]

private type PutResult = Unit | Throwable

trait PutData {
  def key: Key
  def value: Value
  def dataSize: Int = key.length + value.length
}

private final case class Put[F[_]](
    key: Key,
    value: Value,
    signal: Deferred[F, PutResult]
) extends PutData
