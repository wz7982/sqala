import xerial.sbt.Sonatype._

ThisBuild / organization := "com.wz7982"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
    .settings(
        name := "sqala-sbt-codegen",
        sbtPlugin := true,
        organizationName := "sqala",
        organizationHomepage := Some(url("http://wz7982.com/")),
        sonatypeCredentialHost := sonatypeCentralHost,
        sonatypeProfileName := "com.wz7982",
        publishTo := sonatypePublishToBundle.value,
        publishMavenStyle := true,
        licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
        homepage := Some(url("https://github.com/wz7982/sqala")),
        scmInfo := Some(
            ScmInfo(
                url("https://github.com/wz7982/sqala"),
                "scm:git@github.com:wz7982/sqala.git"
            )
        ),
        developers := List(
            Developer(
                id = "wz7982",
                name = "wz7982",
                email = "1064967982@qq.com",
                url = url("https://github.com/wz7982/")
            )
        ),
        libraryDependencies ++= Seq(
            "org.postgresql" % "postgresql" % "42.7.4",
            "com.mysql" % "mysql-connector-j" % "9.1.0"
        ),
        scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
    )
