lazy val commonSettings = Seq(
    organization := "sqala",
    version := "1.0.0",
    scalaVersion := "3.3.1",
    scalacOptions += "-Yexplicit-nulls",
    scalacOptions += "-Wunused:all",
    scalacOptions += "-Ysafe-init",
    scalacOptions += "-deprecation",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"
)

lazy val core = project.in(file("core")).settings(commonSettings)
lazy val test = project.in(file("test")).dependsOn(core).settings(commonSettings)