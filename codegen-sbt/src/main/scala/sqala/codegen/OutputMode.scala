package sqala.codegen

/**
 * Where generated sources are written.
 */
sealed trait OutputMode

object OutputMode {
    /**
     * Write to sbt's managed source directory:
     * `target/scala-<version>/src_managed/main/sqala-codegen`.
     */
    case object Managed extends OutputMode

    /**
     * Write to `src/main/scala/<sqalaSourceDirectory>`.
     */
    case object Source extends OutputMode
}
