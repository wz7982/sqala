package sqala.codegen.sbt

import sbt._
import sbt.Keys._

import java.sql.DriverManager
import sqala.codegen._

/**
 * Generates sqala entity case classes from a PostgreSQL or MySQL schema.
 *
 * Enable the plugin and configure `sqalaDatabaseUrl`, then run:
 *
 * {{{
 * sbt sqalaGenerate
 * }}}
 */
object SqalaCodegenPlugin extends AutoPlugin {
    override def trigger = noTrigger

    override def requires = plugins.JvmPlugin

    object autoImport {
        val sqalaGenerate = taskKey[Seq[File]](
            "Generate sqala entity classes from the database schema"
        )

        val sqalaDatabaseUrl = settingKey[String](
            "JDBC URL used to read the database schema"
        )

        val sqalaDatabaseUser = settingKey[String](
            "Database user used to read the schema"
        )

        val sqalaDatabasePassword = settingKey[String](
            "Database password used to read the schema"
        )

        val sqalaDatabaseDriver = settingKey[String](
            "Optional JDBC driver class; inferred from the URL when empty"
        )

        val sqalaSchema = settingKey[Option[String]](
            "Optional schema name. PostgreSQL defaults to public; MySQL uses the current catalog"
        )

        val sqalaTables = settingKey[Seq[String]](
            "Optional table names; empty means all tables"
        )

        val sqalaOutputPackage = settingKey[String](
            "Package of the generated entity classes"
        )

        val sqalaOutputMode = settingKey[OutputMode](
            "Where generated sources are written: OutputMode.Managed or OutputMode.Source"
        )

        val sqalaSourceDirectory = settingKey[String](
            "Subdirectory under src/main/scala used by OutputMode.Source"
        )

        val sqalaTableNaming = settingKey[TableNaming](
            "Naming style of database table names; generated Scala class names are always PascalCase"
        )

        val sqalaTypeMappings = settingKey[Map[String, String]](
            "Optional SQL type name to Scala type overrides, for example Map(\"uuid\" -> \"String\")"
        )

        val TableNaming = sqala.codegen.TableNaming

        val OutputMode = sqala.codegen.OutputMode
    }

    import autoImport._

    override lazy val projectSettings = Seq(
        sqalaDatabaseUrl := sys.env.getOrElse("SQALA_DATABASE_URL", ""),
        sqalaDatabaseUser := sys.env.getOrElse("SQALA_DATABASE_USER", ""),
        sqalaDatabasePassword := sys.env.getOrElse("SQALA_DATABASE_PASSWORD", ""),
        sqalaDatabaseDriver := "",
        sqalaSchema := None,
        sqalaTables := Seq.empty,
        sqalaOutputPackage := "sqala.generated",
        sqalaOutputMode := OutputMode.Managed,
        sqalaSourceDirectory := "generated",
        sqalaTableNaming := TableNaming.SnakeCase,
        sqalaTypeMappings := Map.empty,
        sqalaGenerate := generateTask.value
    )

    private def generateTask: Def.Initialize[Task[Seq[File]]] = Def.task {
        val log = streams.value.log
        val config = CodegenConfig(
            url = sqalaDatabaseUrl.value.trim,
            user = sqalaDatabaseUser.value,
            password = sqalaDatabasePassword.value,
            driver = Option(sqalaDatabaseDriver.value.trim).filter(_.nonEmpty),
            schema = sqalaSchema.value.map(_.trim).filter(_.nonEmpty),
            tables = sqalaTables.value.map(_.trim).filter(_.nonEmpty),
            outputPackage = sqalaOutputPackage.value.trim,
            tableNaming = sqalaTableNaming.value,
            typeMappings = sqalaTypeMappings.value
        )

        if (config.url.isEmpty) {
            sys.error("sqalaDatabaseUrl is not set")
        }

        val driver = config.driver.getOrElse(driverFor(config.url))
        Class.forName(driver)

        val connection = DriverManager.getConnection(config.url, config.user, config.password)
        try {
            val tables = SchemaReader.read(connection, config)
            val generated = tables.flatMap { table =>
                EntityGenerator.generate(table, config, message => log.warn(message))
            }

            val duplicateNames = generated
                .groupBy(_.className)
                .collect { case (name, items) if items.size > 1 => name }
                .toSeq
                .sorted

            if (duplicateNames.nonEmpty) {
                sys.error(
                    "Multiple tables generate the same class name: " +
                        duplicateNames.mkString(", ")
                )
            }

            val outputMode = sqalaOutputMode.value
            val outputRoot = outputMode match {
                case OutputMode.Managed =>
                    (Compile / sourceManaged).value / "sqala-codegen"
                case OutputMode.Source =>
                    val directory = sqalaSourceDirectory.value.trim
                    if (directory.isEmpty) {
                        sys.error("sqalaSourceDirectory must not be empty in OutputMode.Source")
                    }
                    (Compile / scalaSource).value / directory
            }

            val packageParts =
                if (config.outputPackage.isEmpty) Seq.empty
                else config.outputPackage.split('.').toSeq.filter(_.nonEmpty)

            val manifest = outputRoot / ManifestName
            val previousFiles =
                if (outputMode == OutputMode.Source) readManifest(manifest)
                else Seq.empty

            if (outputMode == OutputMode.Managed) {
                IO.delete(outputRoot)
            }

            if (generated.isEmpty) {
                if (outputMode == OutputMode.Source) {
                    cleanStale(outputRoot, previousFiles, Set.empty)
                    writeManifest(manifest, Seq.empty)
                }
                log.warn("sqala-codegen did not generate any entity classes")
                Seq.empty
            } else {
                val files = generated.map { entity =>
                    val packageDirectory =
                        packageParts.foldLeft(outputRoot)((directory, part) => directory / part)
                    val file = packageDirectory / entity.fileName
                    IO.createDirectory(file.getParentFile)
                    IO.write(file, entity.content)
                    file
                }

                if (outputMode == OutputMode.Source) {
                    val relativeFiles = files.map(file => relativePath(outputRoot, file))
                    cleanStale(outputRoot, previousFiles, relativeFiles.toSet)
                    writeManifest(manifest, relativeFiles)
                }

                log.info(
                    "sqala-codegen generated " + files.size + " entity file(s) into " + outputRoot
                )
                files.toSeq
            }
        } finally {
            connection.close()
        }
    }

    private val ManifestName = ".sqala-codegen-manifest"

    private def relativePath(root: File, file: File): String = {
        file
            .relativeTo(root)
            .map(path => path.getPath.replace('\\', '/'))
            .getOrElse(file.getName)
    }

    private def readManifest(manifest: File): Seq[String] = {
        if (manifest.isFile) {
            IO.readLines(manifest).map(_.trim).filter(_.nonEmpty)
        } else {
            Seq.empty
        }
    }

    private def cleanStale(root: File, previousFiles: Seq[String], currentFiles: Set[String]): Unit = {
        previousFiles.filterNot(currentFiles).foreach { relative =>
            resolveInside(root, relative).foreach(IO.delete)
        }
    }

    private def resolveInside(root: File, relative: String): Option[File] = {
        val file = new File(root, relative)
        val rootPath = root.getCanonicalPath
        val filePath = file.getCanonicalPath
        if (filePath == rootPath || filePath.startsWith(rootPath + java.io.File.separator)) {
            Some(file)
        } else {
            None
        }
    }

    private def writeManifest(manifest: File, files: Seq[String]): Unit = {
        IO.createDirectory(manifest.getParentFile)
        val content =
            if (files.isEmpty) ""
            else files.sorted.mkString("\n") + "\n"
        IO.write(manifest, content)
    }

    private def driverFor(url: String): String = {
        val lower = url.toLowerCase
        if (lower.startsWith("jdbc:postgresql:")) {
            "org.postgresql.Driver"
        } else if (lower.startsWith("jdbc:mysql:")) {
            "com.mysql.cj.jdbc.Driver"
        } else {
            sys.error(
                "Cannot infer the JDBC driver from the URL. Set sqalaDatabaseDriver explicitly."
            )
        }
    }
}
