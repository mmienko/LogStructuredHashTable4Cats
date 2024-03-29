ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "LogStructuredHashTable4Cats",
    libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4",
    libraryDependencies += "com.monovore" %% "decline-effect" % "2.4.1",
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.10.1",
    libraryDependencies += "co.fs2" %% "fs2-io" % "3.10.1",
    libraryDependencies += "com.disneystreaming" %% "weaver-cats" % "0.8.4" % Test,
    testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
    libraryDependencies += "com.disneystreaming" %% "weaver-scalacheck" % "0.8.4" % Test
  )
