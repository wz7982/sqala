lazy val commonSettings = Seq(
    organization := "sqala",
    version := "1.0.0",
    scalaVersion := "3.5.0-RC1",
    scalacOptions += "-Yexplicit-nulls",
    scalacOptions += "-Wunused:all",
    scalacOptions += "-Ysafe-init",
    scalacOptions += "-deprecation",
    scalacOptions += "-experimental",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
)

lazy val core = project.in(file("core")).settings(commonSettings)
lazy val test = project.in(file("test")).dependsOn(core).settings(commonSettings)