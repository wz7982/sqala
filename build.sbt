import xerial.sbt.Sonatype.*

lazy val commonSettings = Seq(
    scalaVersion := "3.9.0",

    version := "0.8.6",

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

lazy val sqala = (project in file(".")).settings(commonSettings).settings(publish / skip := true)
    .aggregate(core, staticIr, static, staticExt, dynamic, jdbc, slf4j, springBootStarter)

lazy val core = project.in(file("core")).settings(commonSettings).settings(name := "sqala-core")

lazy val staticIr = project.in(file("static-ir")).dependsOn(core).settings(commonSettings).settings(name := "sqala-static-ir")

lazy val static = project.in(file("static")).dependsOn(staticIr).settings(commonSettings).settings(name := "sqala-static")

lazy val staticExt = project.in(file("static-ext")).dependsOn(static).settings(commonSettings).settings(name := "sqala-static-ext")

lazy val dynamic = project.in(file("dynamic")).dependsOn(core).settings(commonSettings).settings(name := "sqala-dynamic")

lazy val jdbc = project.in(file("jdbc")).dependsOn(static, dynamic).settings(commonSettings).settings(
    name := "sqala-jdbc",
    Test / parallelExecution := false,
    libraryDependencies ++= Seq(
        "org.postgresql" % "postgresql" % "42.7.4" % Test,
        "com.mysql" % "mysql-connector-j" % "9.2.0" % Test
    )
)

lazy val slf4j = project
    .in(file("slf4j"))
    .dependsOn(jdbc)
    .settings(commonSettings)
    .settings(
        name := "sqala-slf4j",
        libraryDependencies ++= Seq(
            "org.slf4j" % "slf4j-api" % "2.0.17" % Provided,
            "org.slf4j" % "slf4j-simple" % "2.0.17" % Test
        )
    )

lazy val springBootStarter = project
    .in(file("spring-boot-starter"))
    .dependsOn(slf4j)
    .settings(commonSettings)
    .settings(
        name := "sqala-spring-boot-starter",
        scalacOptions --= Seq("-Yexplicit-nulls"),
        libraryDependencies ++= Seq(
            "org.springframework.boot" % "spring-boot-autoconfigure" % "4.0.10" % Provided,
            "org.springframework" % "spring-context" % "7.0.11" % Provided,
            "org.springframework" % "spring-jdbc" % "7.0.11" % Provided,
            "org.springframework" % "spring-tx" % "7.0.11" % Provided,
            "io.micrometer" % "context-propagation" % "1.2.1" % Provided,
            "org.slf4j" % "slf4j-api" % "2.0.17" % Provided,
            "com.h2database" % "h2" % "2.3.232" % Test
        )
    )

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
