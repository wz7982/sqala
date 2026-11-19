import xerial.sbt.Sonatype.*

lazy val commonSettings = Seq(
    scalaVersion := "3.9.0",

    version := "0.8.2",

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

    scalacOptions += "-Wunused:imports",
    scalacOptions += "-Wunused:privates",
    scalacOptions += "-Wunused:locals",
    scalacOptions += "-Wunused:explicits",
    scalacOptions += "-Wsafe-init",
    scalacOptions += "-Yexplicit-nulls",

    libraryDependencies += "org.scalameta" %% "munit" % "1.3.3" % Test
)

lazy val sqala = (project in file(".")).settings(commonSettings)
    .aggregate(core, staticIr, static, staticExt, dynamic, jdbc)

lazy val core = project.in(file("core")).settings(commonSettings).settings(name := "sqala-core")

lazy val staticIr = project.in(file("static-ir")).dependsOn(core).settings(commonSettings).settings(name := "sqala-static-ir")

lazy val static = project.in(file("static")).dependsOn(staticIr).settings(commonSettings).settings(name := "sqala-static")

lazy val staticExt = project.in(file("static-ext")).dependsOn(static).settings(commonSettings).settings(name := "sqala-static-ext")

lazy val dynamic = project.in(file("dynamic")).dependsOn(core).settings(commonSettings).settings(name := "sqala-dynamic")

lazy val jdbc = project.in(file("jdbc")).dependsOn(static, dynamic).settings(commonSettings).settings(name := "sqala-jdbc")

lazy val benchmark = project
    .in(file("benchmark"))
    .dependsOn(jdbc)
    .enablePlugins(JmhPlugin)
    .settings(commonSettings)
    .settings(
        name := "sqala-benchmark",
        publish / skip := true,
        scalacOptions --= Seq("-Yexplicit-nulls"),
        libraryDependencies ++= Seq(
            "com.zaxxer" % "HikariCP" % "6.2.1",
            "org.postgresql" % "postgresql" % "42.7.4",
            "org.mybatis" % "mybatis" % "3.5.19",
            "org.hibernate.orm" % "hibernate-core" % "6.6.4.Final",
            "org.jooq" % "jooq" % "3.19.15",
            "com.typesafe.slick" %% "slick" % "3.6.1",
            "com.alibaba" % "druid" % "1.2.23",
            "org.apache.calcite" % "calcite-core" % "1.38.0",
            "com.github.jsqlparser" % "jsqlparser" % "5.0",
            "org.slf4j" % "slf4j-nop" % "2.0.16"
        )
    )