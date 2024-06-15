lazy val commonSettings = Seq(
    organization := "sqala",
    version := "0.0.1",
    scalaVersion := "3.5.0-RC1",
    scalacOptions += "-Yexplicit-nulls",
    scalacOptions += "-Wunused:all",
    scalacOptions += "-Wsafe-init",
    scalacOptions += "-deprecation",
    scalacOptions += "-experimental",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
)

lazy val core = project.in(file("core")).settings(commonSettings)
lazy val dsl = project.in(file("dsl")).dependsOn(core).settings(commonSettings)
lazy val jdbc = project.in(file("jdbc")).dependsOn(dsl).settings(commonSettings)
lazy val dynamic = project.in(file("dynamic")).dependsOn(core).settings(commonSettings)
lazy val json = project.in(file("json")).dependsOn(core).settings(commonSettings)
