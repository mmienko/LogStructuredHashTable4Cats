package io.lsht

import cats.effect.Deferred

private sealed abstract class Write extends Product with Serializable

private object Write {
  type Result = Unit | Throwable
  // TODO: Formatting needs newline after params
  final case class Put[F[_]](key: Key, value: Value, signal: Deferred[F, Result]) extends Write {
    def dataSize: Int = key.length + value.length
  }

  final case class Delete[F[_]](key: Key, signal: Deferred[F, Result]) extends Write

}
