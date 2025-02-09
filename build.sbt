import xerial.sbt.Sonatype.*

lazy val commonSettings = Seq(
    scalaVersion := "3.6.2",

    version := "0.2.27",

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
    scalacOptions += "-Yexplicit-nulls",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0"
)

lazy val sqala = (project in file(".")).settings(commonSettings)
    .aggregate(core, query, jdbc, data)
lazy val core = project.in(file("core")).settings(commonSettings).settings(name := "sqala-core")
lazy val query = project.in(file("query")).dependsOn(core).settings(commonSettings).settings(name := "sqala-query")
lazy val jdbc = project.in(file("jdbc")).dependsOn(query).settings(commonSettings).settings(name := "sqala-jdbc")
lazy val data = project.in(file("data")).settings(commonSettings).settings(name := "sqala-data")