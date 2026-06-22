package sqala.metadata

import sqala.printer.*

/**
 * A database dialect.
 */
trait Dialect:
    /**
     * Creates a printer instance for this dialect.
     *
     * `standardEscapeStrings` controls string literal escaping: `true` treats
     * backslashes literally (standard behavior, e.g. PostgreSQL, Oracle);
     * `false` uses backslashes as escape characters (e.g. MySQL).
     */
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter

/**
 * MySQL dialect.
 */
object MysqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new MysqlPrinter(standardEscapeStrings)

/**
 * PostgreSQL dialect.
 */
object PostgresqlDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new PostgresqlPrinter(standardEscapeStrings)

/**
 * Oracle dialect.
 */
object OracleDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new OraclePrinter(standardEscapeStrings)

/**
 * SQL Server dialect.
 */
object SqlserverDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new SqlserverPrinter(standardEscapeStrings)

/**
 * SQLite dialect.
 */
object SqliteDialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new SqlitePrinter(standardEscapeStrings)

/**
 * H2 dialect.
 */
object H2Dialect extends Dialect:
    def printer(standardEscapeStrings: Boolean): StandardSqlPrinter =
        new H2Printer(standardEscapeStrings)