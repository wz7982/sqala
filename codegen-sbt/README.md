# sqala sbt codegen

An sbt plugin that reads a PostgreSQL or MySQL schema and generates sqala entity
case classes.

This is an optional tool built separately from the main Scala 3 library because
sbt plugins are compiled with Scala 2.12.

## Add the plugin

For now, publish it locally from this directory:

```bash
sbt publishLocal
```

Then in the project that should generate entities:

```scala
// project/plugins.sbt
addSbtPlugin("com.wz7982" % "sqala-sbt-codegen" % "0.1.0")
```

```scala
// build.sbt
enablePlugins(SqalaCodegenPlugin)

sqalaDatabaseUrl := "jdbc:postgresql://localhost:5432/app"
sqalaDatabaseUser := sys.env("DB_USER")
sqalaDatabasePassword := sys.env("DB_PASSWORD")

// PostgreSQL defaults to public; MySQL uses the current catalog.
sqalaSchema := Some("public")

// Empty means all tables.
sqalaTables := Seq("user", "order")

sqalaOutputPackage := "com.example.domain"

// Managed (default) or Source
sqalaOutputMode := OutputMode.Managed

// Only used by OutputMode.Source:
// files are written to src/main/scala/<sqalaSourceDirectory>/...
sqalaSourceDirectory := "generated"

// SnakeCase (default), CamelCase, PascalCase, or KebabCase.
// This describes the database table names; generated Scala class names are
// always PascalCase.
sqalaTableNaming := TableNaming.SnakeCase
```

Run:

```bash
sbt sqalaGenerate
```

## Output modes

### `OutputMode.Managed` (default)

Generated files are written to:

```text
target/scala-3.x/src_managed/main/sqala-codegen/
```

Running `sqalaGenerate` only writes files; sbt does not compile files under
`sourceManaged` unless they are returned by a source generator. To include them
and generate before every compile:

```scala
Compile / sourceGenerators += sqalaGenerate.taskValue
```

If you do not want every compile to connect to the database, run
`sqalaGenerate` manually and add the generated directory as a source directory:

```scala
Compile / unmanagedSourceDirectories +=
    (Compile / sourceManaged).value / "sqala-codegen"
```

`sbt clean` removes generated files, so they must be regenerated after a clean.

### `OutputMode.Source`

Generated files are written to:

```text
src/main/scala/<sqalaSourceDirectory>/<package path>/
```

For example, with `sqalaSourceDirectory := "generated"` and
`sqalaOutputPackage := "com.example.domain"`:

```text
src/main/scala/generated/com/example/domain/User.scala
```

This directory is under `src/main/scala`, so it is compiled normally. No
`sourceGenerators` or `unmanagedSourceDirectories` setting is required.

Source mode is intended for teams that want to commit and review generated code.
A `.sqala-codegen-manifest` file records the generated files. On the next run,
only files tracked by the manifest are cleaned up; unrelated user files in the
same directory are left alone.

Generated files are overwritten on every run. Do not edit them directly; use
`sqalaTypeMappings`, or move a table out of generation and take ownership of the
entity manually if you need persistent changes.

## Settings

| Setting | Description |
| --- | --- |
| `sqalaGenerate` | Task that reads the schema and writes entity files |
| `sqalaDatabaseUrl` | JDBC URL, for example `jdbc:postgresql://...` or `jdbc:mysql://...` |
| `sqalaDatabaseUser` | Database user |
| `sqalaDatabasePassword` | Database password |
| `sqalaDatabaseDriver` | Optional driver class; inferred from the URL |
| `sqalaSchema` | PostgreSQL schema, or MySQL catalog; PostgreSQL defaults to `public` |
| `sqalaTables` | Tables to generate; empty means all tables |
| `sqalaOutputPackage` | Package of the generated classes |
| `sqalaOutputMode` | `OutputMode.Managed` or `OutputMode.Source` |
| `sqalaSourceDirectory` | Subdirectory under `src/main/scala` for `OutputMode.Source` |
| `sqalaTableNaming` | Database table naming style: `SnakeCase`, `CamelCase`, `PascalCase`, or `KebabCase` |
| `sqalaTypeMappings` | Overrides keyed by SQL type name, for example `Map("uuid" -> "String")` |

The URL, user, and password default to the `SQALA_DATABASE_URL`,
`SQALA_DATABASE_USER`, and `SQALA_DATABASE_PASSWORD` environment variables.

## Table naming

`sqalaTableNaming` describes how the database table names are written. The
generated Scala class name is always PascalCase.

| Value | Database example | Generated class |
| --- | --- | --- |
| `SnakeCase` | `user_profile` | `UserProfile` |
| `CamelCase` | `userProfile` | `UserProfile` |
| `PascalCase` | `UserProfile` | `UserProfile` |
| `KebabCase` | `user-profile` | `UserProfile` |

The raw table name is always preserved in `@table("...")`.

## Generated code

```scala
package com.example.domain

import sqala.metadata.table
import sqala.metadata.primaryKey

@table("user")
case class User(
    @primaryKey id: Long,
    name: String,
    email: Option[String],
    createdAt: java.time.LocalDateTime
)
```

The raw table name is always written to `@table`, and `@column` is added when the
database column name does not round-trip through sqala's default CamelCase to
snake_case conversion. Reserved words and SQL quoting are left to sqala's SQL
generator.

## Supported types

- integer types: `Int`, `Long`
- floating point: `Float`, `Double`
- `BigDecimal`
- `Boolean`
- text: `String`
- `LocalDate`
- `LocalTime`
- `LocalDateTime`
- `OffsetDateTime`
- `OffsetTime`
- `Json` for `json` / `jsonb`
- nullable columns become `Option[T]`

Primary keys use `@primaryKey`. Auto-increment primary keys use both
annotations:

```scala
@primaryKey @autoInc id: Long
```

## Unsupported types

- `BLOB`, `BINARY`, `VARBINARY`: the column is skipped with a warning
- arrays: the column is skipped with a warning
- geometry/geography: the column is skipped with a warning
- database enums: the column is generated as `String` with a warning; change it
  to a Scala enum manually
- `UUID`: generated as `String` with a warning; change it if you provide a UUID
  encoder

If a table's primary key contains an unsupported column, the whole table is
skipped.

Use `sqalaTypeMappings` to override a type name when the default is not what you
want, for example:

```scala
sqalaTypeMappings := Map(
    "uuid" -> "java.util.UUID",
    "bytea" -> "Array[Byte]"
)
```

The generated code may then require additional `JdbcEncoder` / `JdbcDecoder`
instances, which are intentionally not generated.

## Publishing

### Local

```bash
cd codegen-sbt
sbt publishLocal
```

Then use it from another project:

```scala
// project/plugins.sbt
addSbtPlugin("com.wz7982" % "sqala-sbt-codegen" % "0.1.0")
```

### Maven Central

The build is already configured with `sbt-sonatype` and `sbt-pgp`. Reuse the
same Sonatype credentials and PGP key that are used for the main sqala
artifacts.

1. Set the version in `codegen-sbt/build.sbt`.
2. Make sure the PGP key and Sonatype credentials are configured.
3. Publish and release:

```bash
cd codegen-sbt
sbt clean publishSigned
sbt sonatypeBundleRelease
```

The artifact is published as:

```text
com.wz7982:sqala-sbt-codegen_2.12_1.0:<version>
```

Consumers still reference it through `addSbtPlugin` without the suffix:

```scala
addSbtPlugin("com.wz7982" % "sqala-sbt-codegen" % "<version>")
```

If you publish to a private repository instead, override `publishTo` and add
the corresponding credentials.

## Limitations

- PostgreSQL and MySQL only
- base tables only, no views
- no relationship/foreign-key generation
- no incremental schema cache; the task reads the database each time it runs
- generated sources are always deleted and rewritten in the managed directory
