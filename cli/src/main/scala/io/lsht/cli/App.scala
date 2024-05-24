package io.lsht.cli

import cats.Show
import cats.effect.*
import cats.syntax.all.*
import fs2.io.file.Files
import io.lsht.engine.{Database, Key, Value}

import scala.concurrent.duration.*

object App extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    for {
      cwd <- Files[IO].currentWorkingDirectory
      testDbPath = cwd / "test_db"
      _ <- Files[IO].createDirectory(testDbPath)

      exitCode <- Database[IO](
        directory = testDbPath,
        entriesLimit = 5,
        compactionWatchPollTimeout = 5.seconds
      ).use { db =>
        fs2.Stream
          .repeatEval(IO.print("db > ") *> IO.readLine)
          .evalMap { input =>
            // meta commands vs. db commands
            if (input.startsWith("."))
              if (input == ".exit")
                IO.println("shutting down") <* IO.raiseError(Stop)
              else
                IO.println("Unknown meta command")
            else if (input.startsWith("put ")) {
              val keyValue = input.stripPrefix("put ").split(" ", 2)
              db.put(Key(keyValue(0)), Value(keyValue(1)))
            } else if (input.startsWith("get ")) {
              val key = input.stripPrefix("get ")
              db.get(Key(key))
                .flatMap {
                  case Some(value) =>
                    printBytes(value)

                  case None =>
                    IO.println("(nil)")
                }
            } else if (input.startsWith("delete ")) {
              val key = input.stripPrefix("delete ")
              db.delete(Key(key))
            } else if (input.startsWith("keys"))
              db.keys
                .map(_.value)
                .zipWithIndex
                .evalMap((key, i) => IO.print(s"${i + 1})") *> printBytes(key))
                .compile
                .drain
            else
              IO.println("Unknown command")
          }
          .compile
          .drain
          .attempt
          .as(ExitCode.Success)
      }
    } yield exitCode

  private def printBytes(value: Array[Byte]) =
    IO.println(s"\"${new String(value)}\"")

  // Can be cleaner if reads added to None terminated queue
  private object Stop extends Throwable
}
