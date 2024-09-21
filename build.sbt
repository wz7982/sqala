import xerial.sbt.Sonatype.*

lazy val commonSettings = Seq(
    scalaVersion := "3.5.0",

    version := "0.0.22",

    organization := "com.wz7982",

    organizationName := "sqala",

    organizationHomepage := Some(url("http://wz7982.com/")),

    sonatypeCredentialHost := sonatypeCentralHost,

    licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),

    publishTo := sonatypePublishToBundle.value,

    sonatypeProfileName := "com.wz7982",

    publishMavenStyle := true,

    sonatypeProjectHosting := Some(GitHubHosting("wz7982", "sqala", "1064967982@qq.com")),

    homepage := Some(url("https://github.com/wz7982/sqala")),
    scmInfo := Some(
        ScmInfo(
            url("https://github.com/wz7982/sqala"),
            "scm:git@github.com:wz7982/sqala.git"
        )
    ),
    developers := List(
        Developer(id = "wz7982", name = "wz7982", email = "1064967982@qq.com", url = url("https://github.com/wz7982/"))
    ),

    scalacOptions += "-Wunused:all",
    scalacOptions += "-Wsafe-init",
    scalacOptions += "-experimental",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
)

lazy val sqala = (project in file(".")).settings(commonSettings)
    .aggregate(core, dsl, jdbc, dynamic, data)
lazy val core = project.in(file("core")).settings(commonSettings).settings(name := "sqala-core")
lazy val dsl = project.in(file("dsl")).dependsOn(core).settings(commonSettings).settings(name := "sqala-dsl")
lazy val jdbc = project.in(file("jdbc")).dependsOn(dsl).settings(commonSettings).settings(name := "sqala-jdbc")
lazy val dynamic = project.in(file("dynamic")).dependsOn(core).settings(commonSettings).settings(name := "sqala-dynamic")
lazy val data = project.in(file("data")).settings(commonSettings).settings(name := "sqala-data")
